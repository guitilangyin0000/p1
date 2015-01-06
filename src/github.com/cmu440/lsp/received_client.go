package lsp

import (
    "container/list"
    "github.com/cmu440/lspnet"
)

type clientLit struct {
	connId     int
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

