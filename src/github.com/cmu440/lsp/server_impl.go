// Contains the implementation of a LSP server.

package lsp

import (
	"encoding/json"
	"errors"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/cmu440/lspnet"
)

var (
	LOGE = log.New(os.Stderr, "Error ", log.Lmicroseconds|log.Lshortfile)
	LOGV = log.New(os.Stdout, "VERBOSE ", log.Lmicroseconds|log.Lshortfile)
)

const (
	messageMax = 1024
)

type server struct {
	port        int
	conn        *lspnet.UDPConn
	readPend    bool
	clientAlive bool
	closePend   bool
	nextConnID  int
	windowSize  int
	epochLimit  int
	epochMillis int

	epochCountMutex chan struct{}
	epochSig        chan struct{} // signal epoch event

	clientMap map[string]*clientLit
	cIdMap    map[int]string

	closeRequest chan struct{}
	closeReply   chan bool

	writeRequest chan *writeParam
	writeReply   chan bool

	closeAllClient chan struct{} // collect all close confirmation from all clients

	closeConnRequest chan int
	closeConnReply   chan bool

	sShutdown chan struct{} // server is down

	readRequest chan struct{}
	readReply   chan *readServerResult

	reply     chan *readServerResult // used for read from client
	newClient chan *clientLit        // notification of new client

	msgChan chan *msgWrapper
}

type writeParam struct {
	connID  int
	payload []byte
}

type readServerResult struct {
	connID  int
	ok      bool
	payload []byte
}
type msgWrapper struct {
	addr *lspnet.UDPAddr
	msg  *Message
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	s := &server{
		port:            port,
		conn:            nil,
		nextConnID:      1,
		readPend:        false,
		closePend:       false,
		clientAlive:     false,
		epochLimit:      params.EpochLimit,
		epochMillis:     params.EpochMillis,
		clientMap:       make(map[string]*clientLit),
		cIdMap:          make(map[int]string),
		windowSize:      params.WindowSize,
		closeAllClient:  make(chan struct{}),
		epochCountMutex: make(chan struct{}, 1),
		epochSig:        make(chan struct{}),

		closeRequest: make(chan struct{}),
		closeReply:   make(chan bool),

		writeRequest: make(chan *writeParam),
		writeReply:   make(chan bool),

		closeConnRequest: make(chan int),
		closeConnReply:   make(chan bool),

		sShutdown: make(chan struct{}),

		readRequest: make(chan struct{}),
		readReply:   make(chan *readServerResult),

		reply:     make(chan *readServerResult, 1),
		newClient: make(chan *clientLit),

		msgChan: make(chan *msgWrapper),
	}
	go s.run()
	s.epochCountMutex <- struct{}{}
	return s, nil
}

func (s *server) run() {
	laddr, err := lspnet.ResolveUDPAddr("udp", "localhost:"+strconv.Itoa(s.port))
	if err != nil {
		LOGE.Fatalln(err)
	}
	s.conn, err = lspnet.ListenUDP("udp", laddr)
	if err != nil {
		LOGE.Fatalln(err)
	}
	go s.packetReceiver()
	go s.packetProcessor()
	go s.epochHandler()
}

/**
read packets from client
作用：该函数就是从客户端接收信息，然后把信息传入s.msgChan,交给packetProcessor()来处理
*/
func (s *server) packetReceiver() {
	var bytes [messageMax]byte
	for {
		n, addr, err := s.conn.ReadFromUDP(bytes[:])
		if err != nil {
			LOGE.Fatalln(err)
		}
		var msg Message
		json.Unmarshal(bytes[:n], &msg)
		s.msgChan <- &msgWrapper{addr, &msg}
	}
}

/***
**每隔一个epochMillis默认是2000，也就是2000 millisecond，给epochSig传入一个信息
**用来检测每一个client是否还alive以及服务器是否还有alive的client
 */
func (s *server) epochHandler() {
	for {
		select {
		case <-s.sShutdown:
			LOGV.Println("server epoch handler is down")
			return
		case <-time.After(time.Duration(s.epochMillis) * time.Millisecond):
			s.epochSig <- struct{}{}
		}
	}
}

/**
**用来作数据处理以及信息处理
*/
func (s *server) packetProcessor() {
	for {
		select {
		case <-s.epochSig: // 每进来一个信号，检测一次
			LOGV.Println("server epoch start") // server only need to confirm if the client is closed
			s.serverEpoch()
			if s.closePend && !s.clientAlive { // server is close pend and 没有一个client alive
				LOGV.Println("server close after epoch handler")
				close(s.sShutdown) // sShutdown is a channel, close means sShutdown can't read or write data
				s.closeReply <- true
				return
			}
			LOGV.Println("server epoch end, the server is not closed")
		case msgWrap := <-s.msgChan:
			msg := msgWrap.msg
            LOGV.Printf("\nreceive data: %s\n", msg.String())
			switch msg.Type {
			case MsgConnect: // handle connect request from client
				c := s.getClient(msgWrap)
				if c == nil {
					c = s.createClient(msgWrap)
				}
				<-s.epochCountMutex // 不懂
				c.epochCount = 0
				s.epochCountMutex <- struct{}{}
				c.processConn(msg)
				s.clientAlive = true
			case MsgAck:
				c := s.getClient(msgWrap)
				if c == nil {
					break
				}
				<-s.epochCountMutex
				c.epochCount = 0
				s.epochCountMutex <- struct{}{}
				c.processAck(msg)
				if !c.active {
					if c.writeList.Len() == 0 && c.sendDataNext == c.sendDataNotAckEarliest {
						LOGV.Println(c.connID, "discarded now")
						delete(s.clientMap, s.cIdMap[c.connID])
						delete(s.cIdMap, c.connID)
					}
				}
				if s.readPend {
					r := c.readFromServer()
					if r != nil {
						s.readReply <- r
						s.readPend = false
					}
				}

			case MsgData:
				c := s.getClient(msgWrap)
				if c == nil {
					c = s.createClient(msgWrap)
				}
				<-s.epochCountMutex // 类似于锁
                s.epochCountMutex <- struct{}{}
				c.epochCount = 0
				c.processData(msg)
                // check Read() block,如果有因为调用Read()被阻塞，则读一个数据
                // 其实就是Server 不断的读，有可能读的比较快，把缓存里面的读完了
                // 现在处于readPend状态
				if s.readPend {
					r := c.readFromServer()
					if r != nil {
						s.readReply <- r
						s.readPend = false
					}
				}
			}
		case <-s.readRequest:    // 有读数据请求，也就是调用了Read()
			s.readPend = true
			for _, c := range s.clientMap {
				r := c.readFromServer()
				if r != nil {
					LOGV.Println("read something")
					s.readReply <- r
					s.readPend = false
					break
				}
			}
        case req := <-s.writeRequest:
            LOGV.Println("Write case start")
            c, err := s.getClientByID(req.connID)
            if !c.active || err != nil {
                LOGV.Println("server write error");
                s.writeReply<-false
            } else {
                c.writeList.PushBack(req.payload)
                c.processWrite()
                s.writeReply<-true
            }
        case connID := <-s.closeConnRequest:
            c, e := s.getClientByID(connID)
            if c == nil || e != nil {
                s.closeConnReply<-false
            }
            if c.active == false {
                s.closeConnReply<-false
            }
            c.active = false
            c.readList.Init()
            s.closeConnReply<-true
        case <- s.closeRequest:
            s.closePend = true
            if !s.clientAlive {
                LOGV.Println("Close after close")
                close(s.sShutdown)
                s.closeReply <- false
                return
            } else {
                for _, c := range s.clientMap {
                    LOGV.Println("server make ", c.connID, " closed")
                    c.active = false
                }
                s.closeReply <-true
            }
		}
	}
}

/**
**作用:遍历client，看是否有断开连接的
 */
func (s *server) serverEpoch() {
	valid := false // if valid is false, it means server has no one client
	for _, id := range s.cIdMap {
		c := s.clientMap[id]
		if c.closed {
			LOGV.Println(c.connID, "is closed")
			continue
		}
		// 意思是最后发送的数据没有收到ack的号和还没发送的数据号相同，表示所有发送
        // 的数据都接收到了ack,并且现在要发送数据的缓存为空,无数据可发
		if !c.active && c.writeList.Len() == 0 && c.sendDataNotAckEarliest == c.sendDataNext {
			c.closed = true
			LOGV.Println(c.connID, "is closed")
			continue
		}
		LOGV.Println(c.connID, "is alive")
		valid = true
		c.processEpoch()

		// 如果此时处于调用了Read()的状态，则读数据
		if s.readPend {
			r := c.readFromServer()
			if r != nil {
				s.readReply <- r
				s.readPend = false
			}
		}
	}

	s.clientAlive = valid
	if valid {
		LOGV.Println("server has client alive")
	} else {
		LOGV.Println("server has no client alive")
	}
}

func (s *server) createClient(msgWrap *msgWrapper) *clientLit {
	addr := msgWrap.addr
	_, ok := s.clientMap[addr.String()]
	if !ok {
		cLit := newClientLit(s.nextConnID, s.windowSize, s.epochLimit, s.epochMillis, s.conn)
        cLit.addr = addr
        s.clientMap[addr.String()] = cLit
        s.cIdMap[s.nextConnID] = addr.String()
        s.nextConnID++;
        cLit.active = true
	}
	client := s.clientMap[addr.String()]
	return client
}

func (s *server) getClientByID(connID int) (*clientLit, error) {
    cli, ok := s.clientMap[s.cIdMap[connID]]
    if !ok {
        return nil, errors.New("can not find client")
    }
    return cli, nil
}

func (s *server) getClient(msgWrap *msgWrapper) *clientLit {
	addr := msgWrap.addr
	client, ok := s.clientMap[addr.String()]
	if !ok {
		return nil
	}
	return client
}

func (s *server) Read() (n int, payload []byte, e error) {
    LOGV.Println("read called")
    defer LOGV.Println("read end")
    s.readRequest <- struct{}{}
    result := <-s.readReply
    LOGV.Println("Read() get result")
    if !result.ok {
        e = errors.New("server Read(): a connection lost")
        n = result.connID
    } else {
        n = result.connID
        payload = result.payload
    }
    return
}

func (s *server) Write(connID int, payload []byte) error {
    LOGV.Println("server write to ", connID)
    defer LOGV.Println("server write to ", connID, " done")
    s.writeRequest <- &writeParam{connID, payload}
    ok := <-s.writeReply
    if !ok {
        e := errors.New("Server: lost connection")
        return e
    }
    return nil
}

/**
** close client by connID
*/
func (s *server) CloseConn(connID int) error {
    LOGV.Println("CloseConn called")
    defer LOGV.Println("CloseConn end")
    s.closeConnRequest<-connID
    ok := <-s.closeConnReply
    if !ok {
        e := errors.New("connection:" + strconv.Itoa(connID) + " has already been closed")
        return e
    }
    return nil
    }

/**
** close server,感觉没有符合要求，要求是Close()应该等待所有的信息都已经发送并且被acked
** 或者连接断掉，把信息丢弃
*/
func (s *server) Close() (e error) {
    LOGV.Println("Server Colse() called")
    defer LOGV.Println("server close() end")
    s.closeRequest <- struct{}{}
    ok := <-s.closeReply
    if !ok {
        e = errors.New("server has already been closed")
    }
    return
}
