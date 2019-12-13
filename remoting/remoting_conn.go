package remoting

import (
	"github.com/ZhiyangLeeCN/rocketmq-client-go/remoting/protocol"
	"net"
	"sync"
)

type Channel struct {
	net.Conn
	open      bool
	stateLock sync.RWMutex
}

func ChannelConnect(network string, addr string) (*Channel, error) {
	conn, err := net.Dial(network, addr)
	if err != nil {
		return nil, err
	}

	return NewChannel(conn), nil
}

func NewChannel(conn net.Conn) *Channel {
	return &Channel{
		Conn:      conn,
		open:      true,
		stateLock: sync.RWMutex{},
	}
}

func (s *Channel) WriteCmd(cmd *protocol.RemotingCommand) error {

	buf := cmd.EncodeHeaderWithBodyLength()
	if cmd.Body != nil {
		buf.Write(cmd.Body)
	}

	_, err := s.Conn.Write(buf.Bytes())
	if err != nil {
		_ = s.Close()
		return &RemotingSendRequestError{
			err.Error(),
		}
	}
	return nil

}

func (s *Channel) IsActive() bool {
	return s.IsOpen()
}

func (s *Channel) IsOpen() bool {
	defer s.stateLock.RUnlock()
	s.stateLock.RLock()
	return s.open
}

func (s *Channel) Close() error {
	defer s.stateLock.Unlock()
	s.stateLock.Lock()
	if !s.open {
		return nil
	}

	s.open = false
	return s.Conn.Close()
}
