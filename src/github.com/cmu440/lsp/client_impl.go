// Contains the implementation of a LSP client.

package lsp

import (
    "time"
    "encoding/json"
    "container/list"
    "errors"

    "github.com/cmu440/lspnet"
)

type client struct {
	connid int
    hostport string
    conn *lspnet.UDPConn
    active bool
    readPend    bool
    allClean    bool
    closePend    bool

    epochLimit  int
    epochMillis int
    windowSize  int
    epochCount  int

    epochCountMutex chan struct{}

    // send
    sendDataNotAckEarliest int                 // first send packet not acknowledged
    sendDataNext int                // next sequence number to be sent
    sendDataCache   map[int]*Message    // cached for sent packets
    sendDataAckCache    map[int]bool    // 表示send数据之后收到ack的缓存，也就是若收到后则设置为true

    // receive
    receiveDataNext int
    receiveRecent   *list.List
    receiveDataCache    map[int]*Message

    // channel
    connChan chan int               // notify connection
    msgChan chan *Message           // transfer message to processor
    epochSig chan struct{}          // notify epoch event
    shutdown chan struct{}          // notify the components of current client

    epochOutOfLimit chan struct{}

    writeRequest chan []byte
    writeReply chan bool
    readRequest chan struct{}
    readReply   chan readResult
    closeRequest chan struct{}
    closeReply  chan bool

    writeList *list.List
    readList *list.List

}

type readResult struct {
    succ bool
    data []byte
}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, params *Params) (Client, error) {
    cli := new(client)
    cli.conn = nil
    cli.connid = 0
    cli.active = true
    cli.allClean = false
    cli.closePend = false
    cli.closeReply = make(chan bool)
    cli.closeRequest = make(chan struct{})
    cli.connChan = make(chan int)
    cli.epochCount = 0
    cli.epochCountMutex = make(chan struct{}, 1)
    cli.epochLimit = params.EpochLimit
    cli.epochMillis = params.EpochMillis
    cli.windowSize = params.WindowSize
    cli.epochOutOfLimit = make(chan struct{})
    cli.msgChan = make(chan *Message)
    cli.hostport = hostport
    cli.epochSig = make(chan struct{})
    cli.readList = list.New()
    cli.writeList = list.New()
    cli.readPend = false
    cli.readReply = make(chan readResult)
    cli.receiveDataCache = make(map[int]*Message)
    cli.receiveDataNext = 1
    cli.receiveRecent = list.New()
    cli.sendDataAckCache = make(map[int]bool)
    cli.sendDataNotAckEarliest = 0 // connect 收到ack之后加1, 为0表示最开始发送的数据为connect为0，收到的ack是0
    cli.sendDataCache = make(map[int]*Message)
    cli.sendDataNext = 1
    cli.shutdown = make(chan struct{})
    cli.writeReply = make(chan bool)
    cli.writeRequest = make(chan []byte)

    // 类似于一把锁，当下次没有进行要更改cli.epochCount的时候，必须能够从cli.epochCountMutex读到东西，
    // 若阻塞，则说明别的协程正在更改epochCountMutex
    cli.epochCountMutex <- struct{}{}

    go cli.run()
    select {    // select 会在其中任意一个case命中的时候跳出
    case _, ok := <-cli.connChan:
        if !ok {
            cli = nil
            err := errors.New("can not connect to server")
            return cli, err
        }
    case <-cli.shutdown:
        return nil, nil
    }
    return cli, nil
}

func (c *client) run() {
    addr, err := lspnet.ResolveUDPAddr("udp", c.hostport+":")
    if err != nil {
        LOGE.Fatalln("run resolveudpaddr error")
    }
    conn, err := lspnet.DialUDP("udp", nil, addr)
    if err != nil {
        LOGE.Fatalln("run lspnet dialUDP error")
    }
    c.conn = conn
    connmsg := NewConnect() // 返回的是*Message
    c.sendDataCache[0] = connmsg
    msg, err := json.Marshal(connmsg)
    if err != nil {
        LOGE.Println(err)
    }

    _, err = c.conn.Write(msg)
    if err != nil {
        LOGE.Println(err)
    }

    go c.packetReceiver()
    go c.processPacket()
    go c.epochHandler()
}

func (c *client) packetReceiver() {
    for {
        var msg [messageMax]byte
        n, err := c.conn.Read(msg[:])
        if err != nil {
            LOGE.Println(err)
        }
        connMsg := new(Message)
        json.Unmarshal(msg[:n], connMsg)
        c.msgChan<-connMsg
    }
}

func (c *client) processPacket() {
    for {
        select {
        case msg := <-c.msgChan:
            <-c.epochCountMutex
            c.epochCount = 0
            c.epochCountMutex<-struct{}{}
            if !c.active && c.allClean {
                close(c.shutdown)
                c.closeReply<-true // 表示connection has been already closed
                return
            }
            LOGV.Println("received new packet " + msg.String())
            switch msg.Type {
            case MsgAck:
                c.processAck(msg)
                if c.allClean && c.closePend {
                    close(c.shutdown)
                    c.closeReply<-true
                    return
                }
            case MsgData:
                c.processData(msg)
            case MsgConnect:
                LOGV.Println("shouldn't be here, connect to client")
            }
        case <-c.epochSig:
            c.processEpoch()
            if c.allClean {
                close(c.shutdown)
                c.closeReply<-true
                return
            }
        case bytes := <-c.writeRequest:
            c.writeList.PushBack(bytes)
            c.processWrite()
            c.writeReply <-true
            if c.allClean {
                close(c.shutdown)
                return
            }
        case <-c.readRequest:
            c.readPend = true
            c.processRead()
        case <-c.closeRequest:
            LOGV.Println("Client close()")
            if !c.active && c.allClean {
                c.closeReply<-false
            }
            c.closePend = true
            if c.allClean {
                close(c.shutdown)
                c.closeReply <- true
                return
            }
        case <-c.epochOutOfLimit:
            if c.active {
                LOGV.Println("Epoch limit reached")
                c.active = false
                c.allClean = true
                c.processRead()
                close(c.shutdown)
                return
            } else {
                c.allClean = true
                if c.closePend {
                    if c.readPend {
                        c.readReply <- readResult{false, nil}
                    }
                    LOGV.Println(c.connid, " close after epoch")
                    close(c.shutdown)
                    c.closeReply <- false
                    return
                }
                LOGV.Println("ignore duplicate epoch limit outreach")
            }
        }
    }
}

func (c *client) epochHandler() {
    for {
        select {
        case <-time.After(time.Duration(c.epochMillis)*time.Millisecond): // 每隔一个限定时间则把缓存的刚发送的data和ack都重发一次
            c.epochSig<-struct{}{}
            <-c.epochCountMutex
            c.epochCount++
            c.epochCountMutex<-struct{}{}
            if c.epochCount > c.epochLimit {
                c.epochOutOfLimit<-struct{}{}
            }
        case <-c.shutdown:
            return

        }
    }
}

func (c *client) processEpoch() {
    if c.sendDataNotAckEarliest == 0 { // 说明连接没成功
        connect := NewConnect()
        clientWrite(c.conn, connect)
    }

    for sn := c.sendDataNotAckEarliest; sn < c.sendDataNotAckEarliest + c.windowSize; sn++ {
        msg, ok := c.sendDataCache[sn]
        if ok {
            // resend data
            clientWrite(c.conn, msg)
        }
    }

    s := ""
    for curr := c.receiveRecent.Front(); curr != nil; curr = curr.Next() {
        s += " " + curr.Value.(*Message).String()
    }
    LOGV.Println(c.connid, " client recent : " + s)

    for curr := c.receiveRecent.Front(); curr != nil; curr = curr.Next() {
        // resend ack
        ack := NewAck(curr.Value.(*Message).ConnID, curr.Value.(*Message).SeqNum)
        clientWrite(c.conn, ack)
    }
    // 不太懂
    c.processWrite()
}
/**
** when new ack arrived
*/
func (c *client) processAck(msg *Message) {
    if msg.SeqNum == c.sendDataNotAckEarliest {
        if msg.SeqNum == 0 { // client send connect has been acked，只有connect返回的ack的SeqNum = 0
            c.connid = msg.ConnID
            c.connChan <- 1
            LOGV.Println("connection confirmed")
        }
        LOGV.Println(msg.ConnID, " get ", msg)
        delete(c.sendDataAckCache, c.sendDataNotAckEarliest)  // delete一个不存在的key值不会出错
        delete(c.sendDataCache, c.sendDataNotAckEarliest)
        c.sendDataNotAckEarliest++
        _, ok := c.sendDataAckCache[c.sendDataNotAckEarliest]
        for ok {
            delete(c.sendDataAckCache, c.sendDataNotAckEarliest)
            delete(c.sendDataAckCache, c.sendDataNotAckEarliest)
            c.sendDataNotAckEarliest++
            _, ok = c.sendDataAckCache[c.sendDataNotAckEarliest]
        }
        c.processWrite()
    } else if msg.SeqNum > c.sendDataNotAckEarliest {
        c.sendDataAckCache[msg.SeqNum] = true
        delete(c.sendDataAckCache, msg.SeqNum)
    }
}

/**
** call Write()
*/
func (c *client) processWrite() {
    // 把所有writeList里面的数据都传出去
    for c.sendDataNext < c.sendDataNotAckEarliest + c.windowSize {
        if c.writeList.Len() > 0 {
            payload := c.writeList.Front().Value.([]byte)
            msg := NewData(c.connid, c.sendDataNext, payload)
            c.sendDataCache[c.sendDataNext] = msg
            clientWrite(c.conn, msg)
            c.sendDataNext++
            c.writeList.Remove(c.writeList.Front())
        } else {    // 说明没数据了
            if !c.active && c.writeList.Len() == 0 && c.sendDataNext == c.sendDataNotAckEarliest {
                LOGV.Println(c.connid, " clean at ", c.sendDataNext)
                c.allClean = true
            }
            return
        }
    }

    if !c.active && c.writeList.Len() == 0 && c.sendDataNotAckEarliest == c.sendDataNext {
        LOGV.Println(c.connid, " clean at ", c.sendDataNext)
        c.allClean = true
    }
    return
}

/**
** call Read()
*/
func (c *client) processRead() {
    if c.readPend {
        if !c.active {
            LOGV.Println(c.connid, "is dead")
            c.readReply<-readResult{false, nil}
        } else if c.readList.Len() > 0 {
            bytes := c.readList.Front().Value.([]byte)
            c.readList.Remove(c.readList.Front())
            c.readReply<-readResult{true, bytes}
            c.readPend = false
        } else {
            LOGV.Println("Read list is empty")
        }
    }
}

func (c *client) processData(msg *Message) {
    LOGV.Println("Client receive ", msg)
    if msg.SeqNum >= c.receiveDataNext {
        if msg.SeqNum == c.receiveDataNext {
            ok := true
            c.receiveDataCache[c.receiveDataNext] = msg
            for ok {
                // readList 和 receiveRecent 作用不同, 接收到不代表已经被调用Read()读取掉，
                // 有可能readList里面存的元素个数比receiveRecent多，因为receiveRecent最大为窗口大小
                c.readList.PushBack(c.receiveDataCache[c.receiveDataNext].Payload)
                delete(c.receiveDataCache, c.receiveDataNext)
                ack := NewAck(c.connid, c.receiveDataNext)
                clientWrite(c.conn, ack)
                c.receiveDataNext++
                _, ok = c.receiveDataCache[c.receiveDataNext]
            }
        } else {  // 感觉窗口好像没发挥作用，这里凡是大于期待接收的data序列号都会缓存起来，并不会因为窗口而丢弃
            c.receiveDataCache[msg.SeqNum] = msg
            ack := NewAck(c.connid, msg.SeqNum)
            clientWrite(c.conn, ack)
        }
    }
    dup := false
    mSg := c.receiveRecent.Front()  // mSn 就是为了得到序列号最小的那个，如果接收窗里面没有了位置，就把已经接收的序列号最小的删除
    for curr := c.receiveRecent.Front(); curr != nil; curr = curr.Next() {
        if curr.Value.(*Message).SeqNum < mSg.Value.(*Message).SeqNum {
            mSg = curr
        }
        if curr.Value.(*Message).SeqNum == msg.SeqNum {
            dup = true
            break
        }
    }

    if !dup {
        if c.receiveRecent.Len() < c.windowSize {
            c.receiveRecent.PushBack(msg)
        } else if mSg.Value.(*Message).SeqNum != msg.SeqNum {
            c.receiveRecent.PushBack(msg)
            c.receiveRecent.Remove(mSg);
        }
    }
}


func clientWrite(conn *lspnet.UDPConn, msg *Message) {
    rmsg, err := json.Marshal(msg)
    if err != nil {
        LOGE.Println(err)
        return
    }
    conn.Write(rmsg)
}

func (c *client) ConnID() int {
	return c.connid
}

func (c *client) Read() (d []byte, e error) {
    c.readRequest<-struct{}{}
    r := <-c.readReply
    if !r.succ {
        e = errors.New("read wrong")
        return
    }
    d = r.data
	return
}

func (c *client) Write(payload []byte) (e error) {
	c.writeRequest <-payload
    r := <-c.writeReply
    if !r {
        e = errors.New("Write: connection to server died")
    }
    return
}

func (c *client) Close() (e error) {
    LOGV.Println(c.connid, " Close() called")
    defer LOGV.Println(c.connid, "Close() end")
    c.closeRequest <- struct{}{}
    r := <-c.closeReply
    if !r {
        e = errors.New("Client: has been closed")
        return
    }
    return errors.New("closed success")
}
