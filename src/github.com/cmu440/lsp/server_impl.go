// Contains the implementation of a LSP server.

package lsp

import (
	"time"
	"encoding/json"
	"container/list"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"strconv"

	"github.com/cmu440/lspnet"
)

var (
	LOGE = log.New(os.Stderr, "Error ", log.Lmicroseconds|log.Lshortfile)
	LOGV = log.New(ioutil.Discard, "VERBOSE ", log.Lmicroseconds|log.Lshortfile)
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

	sShutdown chan struct{}  // server is down

	readRequest chan struct{}
	readReply   chan *readServerResult

	reply     chan *readServerResult // used for read from client
	newClient chan *clientLit        // notification of new client

	msgChan chan *msgWrapper
}

type writeParam struct {
	connId  int
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
func NewServer(port int, params *Params) (server, error) {
	s = &server{
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
*/
func (s *server) packetReceiver() {
	var bytes [messageMax]byte
	for {
		n, addr, err := s.conn.ReadFromUDP(bytes)
		if err != nil {
			LOGE.Fatalln(err)
		}
		var msg Message
		json.Unmarshal(bytes[:n], &msg)
		s.msgChan <- &msgWrapper{addr, &msg}
	}
}

/**
	每隔一个epochMillis，给epochSig传入一个信息
*/
func (s *server) epochHandler() {
	for {
		select {
			case <-s.sShutdown:
			LOGV.Println("server epoch handler is down")
			break
        case <-time.After(time.Duration(s.epochMillis*time.Millisecond)):
			s.epochSig <- struct{}{}

		}
	}

}

func (s *server) packetProcessor() {
	for {
		select {
			case <-s.epochSig :
			LOGV.Println("server epoch start") // server only need to confirm if the client is closed
			s.serverEpoch()
			if s.closePend && !s.clientAlive { // server is close pend and 没有一个client alive
				LOGV.Println("server close after epoch handler")
				close(s.sShutdown)    // sShutdown is a channel, close means sShutdown can't read or write data
				s.closeReply <-true
				return
			}
			LOGV.Println("server epoch end, the server is not closed")
			case msgWrap := <-s.msgChan :
				msg := msgWrap.msg
				switch msg {
					case MsgConnect:	// handle connect request from client
						c := s.getClient(msgWrap)
						if c == nil {
							c :=s.createClient(msgWrap)
						}
						<-s.epochCountMutex // 不懂
                        c.epochCount = 0
                        s.epochCountMutex <-struct{}{}
                        c.processConn(msg)
                        s.clientAlive = true
                    case MsgAck:
                        c := s.getClient(msgWrap)

					case MsgData:
				}
			case <-s.readRequest:

		}
	}
}

/**
	serverEpoch 用来检测client是否有关闭的
*/
func (s *server) serverEpoch() {
	valid := false // if valid is false, it means server has no one client
	for _, id = range s.cIdMap {
		conn := s.clientMap[id]
		if c.closed {
			LOGV.Println(c.connId, "is closed")
			continue
		}
		// commnet to do,sendDataNext he sendAckNext 是不是一起维护的,感觉应该是同一个
		if !c.active && c.writeList.len() == 0 && c.sendAckNext == c.sendDataNext {
			c.closed = true
			LOGV.Println(c.connId, "is closed")
			continue
		}
		LOGV.Println(c.connId, "is alive")
		valid := true
		c.processEpoch()

		// 这里就不太明白了
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

func (s *server) Read() (int, []byte, error) {
	// TODO: remove this line when you are ready to begin implementing this method.
	select {} // Blocks indefinitely.
	return -1, nil, errors.New("not yet implemented")
}

func (s *server) Write(connID int, payload []byte) error {
	return errors.New("not yet implemented")
}

func (s *server) CloseConn(connID int) error {
	return errors.New("not yet implemented")
}

func (s *server) Close() error {
	return errors.New("not yet implemented")
}

