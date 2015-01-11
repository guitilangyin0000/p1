package lsp

import (
	"container/list"
    "encoding/json"

	"github.com/cmu440/lspnet"
)

type clientLit struct {
	connID     int  // 该连接的号，和client端的应该是一样的
	windowSize int  // server windowSize
	active     bool
	closed     bool
	readPend   bool  // 是否属于调用了Read的阻塞状态
	conn       *lspnet.UDPConn

	epochMillis int
	epochLimit  int

	epochCount      int
	epochCountMutex chan struct{}

	addr         *lspnet.UDPAddr
	sendAckNext  int  // sendAckNext表示下一个预期接收到的ack号，其实就相当于现在还没有接受到ack最小发送data序列号，例如连接后server先发{data，id, 2, payload}，然后client回复{ack，id, 2}（这个2是从接收到的信息读到的），然后server的接收到的号(2)如果正好等于server.sendAckNext时，sendAckNext++
	sendDataNext int  // 每发送一次data加1
	closeChan    chan int

	conChan chan *Message

	sendDataCache map[int]*Message  // 表示发送的数据的缓存，如果对应的ack号接收到，则从这个里面删除，在sendAckCache里设为true
	sendAckCache  map[int]bool    // 表示发送的data是否接收到ack
	receiveRecent *list.List

	recDataNext  int  // 每收到一次data加1
	recDataCache map[int]*Message

	writeList *list.List // data to be written,有可能server 调用了server.Write(connID, payload)了多次但都还没发送出去，需要暂时保存一下
	readList  *list.List // data to be read by server
}

/**
**一个clientLit就是server端的与一个client对应的处理器
*/
func newClientLit(connID int, wSize int, eLimit int, eMillis int, conn *lspnet.UDPConn) *clientLit {
    cli := clientLit{
        connID:       connID,
        active:       false,
        closed:       false,
        readPend:     false,
        conn:         conn,
        epochLimit:   eLimit,
        epochMillis:  eMillis,
        sendAckNext:  0,    // sendAckNext 表示server发送的数据并且没被回复ack的最小序列号
        sendDataNext: 1,    // sendDataNext 表示下一个要发送的数据的序列号
        addr:         nil,
        closeChan:    make(chan int),
        conChan:      make(chan *Message),
        recDataNext:  1,
        windowSize:   wSize,
        recDataCache: make(map[int]*Message),

        epochCountMutex: make(chan struct{}, 1),

        sendDataCache: make(map[int]*Message),
        sendAckCache:  make(map[int]bool),
        receiveRecent: list.New(),

        writeList: list.New(),
        readList:  list.New(),
    }
    cli.epochCountMutex <- struct{}{}
    return &cli
}
func (c *clientLit) sendAck(sn int) {
    LOGV.Printf("%d send ack to client\n", c.connID)
    ack := NewAck(c.connID, sn)
    write(c.conn, &msgWrapper{c.addr, ack})
}
func write(conn *lspnet.UDPConn, m *msgWrapper) {
    msg, err := json.Marshal(m.msg)
    if err != nil {
        LOGE.Println(err)
    }

    n, err := conn.WriteToUDP(msg, m.addr)
    if err != nil {
        LOGV.Printf("clientLit %d write %d bytes to client\n", m.msg.ConnID, n)
        LOGE.Fatalln("clientLit write error")
    }
    LOGV.Printf("clientLit %d write %d bytes %s to client\n", m.msg.ConnID, n, msg)
}
/**
** process new connection
 */
func (c *clientLit) processConn(msg *Message) {
    if c.sendAckNext == 0 {   // 因为初始化是0，所以等于0表示是连接的请求
        ack := NewAck(c.connID, 0)
        c.sendAckNext++
        write(c.conn, &msgWrapper{c.addr, ack})
    } else {
        LOGV.Println("conn has already connected, drop for ", c.connID)
    }
}

/**
** 每收到一个ack
*/
func (c *clientLit) processAck(msg *Message) {
    switch {
    case msg.SeqNum == c.sendAckNext:    // 如果client回复的ack的序列号是希望接收到的ack号,则删除最近发送数据的那个号
        delete(c.sendAckCache, c.sendAckNext)
        delete(c.sendDataCache, c.sendAckNext)
        c.sendAckNext++
        _, ok := c.sendAckCache[c.sendAckNext]
        for ok {
            delete(c.sendAckCache, c.sendAckNext)
            delete(c.sendDataCache, c.sendAckNext)
            c.sendAckNext++
            _, ok = c.sendAckCache[c.sendAckNext]
        }
        c.processWrite()
    case msg.SeqNum > c.sendAckNext:    // 如果client回复的ack的序列号大于希望接收的ack号，则把缓存接收的ack设为true
    // 并且把可能重发的数据删除
        c.sendAckCache[msg.SeqNum] = true
        delete(c.sendDataCache, msg.SeqNum)
    }
}

/**
** 处理一个新接收到的数据:如果接收到的数据小于希望接收到的最小值，则丢弃（说明已经接收过了）,
** 如果等于，则滑动窗口，如果大于，则缓存之，并且都会发送ack,然后判断数据是否已接收过
** 遍历接收数据缓存窗，顺便得到序列号最小的msg,如果缓存窗没满，则将新接收的数据缓存，如果
** 已经满了，则把序列号最小的去除
*/
func (c *clientLit) processData(msg *Message) {
    if msg.SeqNum >= c.recDataNext {
        if msg.SeqNum == c.recDataNext {  //收到的数据正好是希望收到的数据，则将接收数据窗口滑动
            c.readList.PushBack(msg.Payload)
            go c.sendAck(c.recDataNext)
            c.recDataNext++
            m, ok := c.recDataCache[c.recDataNext]
            for ok {
                delete(c.recDataCache, c.recDataNext)
                go c.sendAck(c.recDataNext)
                c.recDataNext++
                c.readList.PushBack(m.Payload)
                m, ok = c.recDataCache[c.recDataNext]
            }
        } else {
            // Cache
            c.recDataCache[msg.SeqNum] = msg
            go c.sendAck(msg.SeqNum)
        }
        dup := false
        mSn := c.receiveRecent.Front() // 最近收到的数据的第一个,遍历缓存的最近接收的数据列表
        // 如果刚接收的数据是重复的，break
        for curr := c.receiveRecent.Front(); curr != nil; curr = curr.Next() {
            m := curr.Value.(*Message)
            if m.SeqNum < mSn.Value.(*Message).SeqNum {
                mSn = curr
            }
            if m.SeqNum == msg.SeqNum {
                dup = true
                break
            }
        }
        if !dup {
            if c.receiveRecent.Len() < c.windowSize {
                c.receiveRecent.PushBack(msg)
            } else if mSn.Value.(*Message).SeqNum != msg.SeqNum {
                c.receiveRecent.PushBack(msg)
                c.receiveRecent.Remove(mSn)
            }
        }
    }

}

/**
**  c.processEpoch()只有在server触发了定时器，并且在server中遍历所有连接，判断如果是还没有关闭的连接，则调用c.processEpoch()
**  c.epochCount 会在每接收到一次信息的时候置0
*/
func (c *clientLit) processEpoch() {
    // 因为server需要发送的只有ack 和data, data 是主动发送
    // for sending, resend unacked data    c.sendAckNext 表示server发送的数据并且没被回复ack的最小号
    for sn := c.sendAckNext; sn < c.sendAckNext + c.windowSize; sn++ {
        msg, ok := c.sendDataCache[sn]
        if ok {
            LOGV.Println("Resend", msg)
            write(c.conn, &msgWrapper{c.addr, msg})
        }
    }
    // 检查接收到的data是否都回复了ack
    s := ""
    for curr := c.receiveRecent.Front(); curr != nil; curr = curr.Next() {
        s += " " + curr.Value.(*Message).String()
    }
    LOGV.Println("Recent receive " + s)
    // 对于最近接收到的数据，重发ack
    for curr := c.receiveRecent.Front(); curr != nil; curr = curr.Next() {
        m := curr.Value.(*Message)
        go c.sendAck(m.SeqNum)
    }
    c.processWrite()  // 不懂
    var cnt int
    <-c.epochCountMutex // 这就是一个类似于锁的功能,防止server 里面的c.epochCount = 0和下面的这句冲突
    c.epochCount++
    cnt = c.epochCount
    c.epochCountMutex <- struct{}{}
    LOGV.Println(c.connID, " Current epochCount : ", cnt)
    if cnt > c.epochLimit {
        c.active = false
    }
}

/**
** 相当于从本连接的read缓存里读一条数据出来,还只能读前面的，后面的数据来了
** 但前面的没有收到也不能读后面的
*/
func (c *clientLit) readFromServer() *readServerResult {
	if c.closed {
        return nil
    }
    if c.active {
        if c.readList.Len() > 0 {
            p := c.readList.Front().Value.([]byte)
            c.readList.Remove(c.readList.Front())
            return &readServerResult{c.connID, true, p}
        } else {
            LOGV.Println(c.connID, " server empty list!")
            return nil
        }
    } else {
        if c.readList.Len() > 0 {
            p := c.readList.Front().Value.([]byte)
            c.readList.Remove(c.readList.Front())
            return &readServerResult{c.connID, true, p}
        } else {
            c.closed = true
            return &readServerResult{c.connID, false, nil}
        }
    }
}

func (c *clientLit) processWrite() {
    for c.sendDataNext < c.sendAckNext + c.windowSize {
        if c.writeList.Len() > 0 {
            payload := c.writeList.Front().Value.([]byte)
            msg := NewData(c.connID, c.sendDataNext, payload)
            c.sendDataCache[c.sendDataNext] = msg
            write(c.conn, &msgWrapper{c.addr, msg})
            c.sendDataNext++
            c.writeList.Remove(c.writeList.Front())
        } else {
            return
        }
    }
}
