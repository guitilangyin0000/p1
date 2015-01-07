package lsp

import (
	"container/list"
    "encoding/json"

	"github.com/cmu440/lspnet"
)

type clientLit struct {
	connID     int
	windowSize int
	active     bool
	closed     bool
	readPend   bool
	conn       *lspnet.UDPConn

	epochMillis int
	epochLimit  int

	epochCount      int
	epochCountMutex chan struct{}

	addr         *lspnet.UDPAddr
	sendAckNext  int
	sendDataNext int
	closeChan    chan int

	conChan chan *Message

	sendDataCache map[int]*Message
	sendAckCache  map[int]bool
	receiveRecent *list.List

	recDataNext  int
	recDataCache map[int]*Message

	writeList *list.List // data to be written
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
        sendAckNext:  0,
        sendDataNext: 1,
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
    ack := NewAck(c.connID, sn)
    write(c.conn, &msgWrapper{c.addr, ack})
}
func write(conn *lspnet.UDPConn, m *msgWrapper) {
    msg, err := json.Marshal(m.msg)
    if err != nil {
        LOGE.Println(err)
    }

    conn.WriteToUDP(msg, m.addr)
}
/**
** process new connection
 */
func (c *clientLit) processConn(msg *Message) {
    
}

func (c *clientLit) processAck(msg *Message) {

}

func (c *clientLit) processData(msg *Message) {

}

func (c *clientLit) processEpoch() {

}

func (c *clientLit) readFromServer() *readServerResult {
	return nil
}

func (c *clientLit) processWrite() {
}
