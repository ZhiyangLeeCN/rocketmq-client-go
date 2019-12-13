package remoting

import "fmt"

type RemotingConnectError struct {
	s string
}

func NewRemotingConnectError(addr string) error {
	return &RemotingConnectError{
		s: fmt.Sprintf("connect to %s failed", addr),
	}
}
func (s *RemotingConnectError) Error() string {
	return s.s
}

type RemotingTimeoutError struct {
	s string
}

func NewRemotingTimeoutError(addr string, timeoutMillis int64) error {
	return &RemotingTimeoutError{
		s: fmt.Sprintf("RemotingTimeoutError<%s> timeout, %d(ms)", addr, timeoutMillis),
	}
}
func (s *RemotingTimeoutError) Error() string {
	return s.s
}

type RemotingSendRequestError struct {
	s string
}

func (s *RemotingSendRequestError) Error() string {
	return s.s
}

type RemotingTooMuchRequestError struct {
	s string
}

func (s *RemotingTooMuchRequestError) Error() string {
	return s.s
}
