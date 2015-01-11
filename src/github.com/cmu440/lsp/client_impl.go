// Contains the implementation of a LSP client.

package lsp

import (
    "container/list"
    "errors"
    "github.com/cmu440/lspnet"
)

type client struct {
	connID int
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
    sendAckNext int                 // first send packet not acknowledged
    sendDataNext int                // next sequence number to be sent
    sendDataCache   map[int]*Message    // cached for sent packets
    sendAckCache    map[int]bool    // 表示send数据之后收到ack的缓存，也就是若收到后则设置为true

    // receive
    receiveDataNext int
    receiveRecent   *list.List
    receiveDataCache    map[int]*Message

    // channel
    connChan chan int               // notify connection
    msgChan chan *Message           // transfer message to processor
    epochSig chan struct{}          // notify epoch event
    shutdown chan struct{}          // notify the components of current client


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
	return nil, errors.New("not yet implemented")
}

func (c *client) ConnID() int {
	return -1
}

func (c *client) Read() ([]byte, error) {
	// TODO: remove this line when you are ready to begin implementing this method.
	select {} // Blocks indefinitely.
	return nil, errors.New("not yet implemented")
}

func (c *client) Write(payload []byte) error {
	return errors.New("not yet implemented")
}

func (c *client) Close() error {
	return errors.New("not yet implemented")
}
