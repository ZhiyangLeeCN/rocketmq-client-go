package remoting

import (
	"time"

	"github.com/ZhiyangLeeCN/rocketmq-client-go/helper"
	"github.com/ZhiyangLeeCN/rocketmq-client-go/helper/atomic"
	"github.com/ZhiyangLeeCN/rocketmq-client-go/remoting/protocol"
)

type InvokeCallback func(responseFuture *ResponseFuture)

type RequestProcessor interface {
	ProcessRequest(channel *Channel, request *protocol.RemotingCommand) (*protocol.RemotingCommand, error)
	RejectRequest() bool
}

type SemaphoreReleaseOnlyOnce struct {
	released  atomic.Bool
	semaphore *atomic.Semaphore
}

func NewSemaphoreReleaseOnlyOnce(semaphore *atomic.Semaphore) *SemaphoreReleaseOnlyOnce {
	return &SemaphoreReleaseOnlyOnce{
		released:  atomic.MakeBool(false),
		semaphore: semaphore,
	}
}

func (s *SemaphoreReleaseOnlyOnce) Release() {
	if s.semaphore != nil {
		if s.released.CAS(false, true) {
			s.semaphore.Release(1)
		}
	}
}

type ResponseFuture struct {
	opaque                  int32
	processChannel          *Channel
	timeoutMillis           int64
	invokeCallback          InvokeCallback
	beginTimestamp          int64
	once                    *SemaphoreReleaseOnlyOnce
	executeCallbackOnlyOnce atomic.Bool
	responseCommand         *protocol.RemotingCommand
	sendRequestOK           bool
	cause                   error
	done                    chan bool
}

func NewDefaultResponseFuture(channel *Channel, opaque int32, timeoutMillis int64,
	invokeCallback InvokeCallback, once *SemaphoreReleaseOnlyOnce) *ResponseFuture {
	return &ResponseFuture{
		opaque:                  opaque,
		processChannel:          channel,
		timeoutMillis:           timeoutMillis,
		invokeCallback:          invokeCallback,
		beginTimestamp:          helper.CurrentTimeMillis(),
		executeCallbackOnlyOnce: atomic.MakeBool(false),
		sendRequestOK:           true,
		once:                    once,
		done:                    make(chan bool),
	}
}

func (s *ResponseFuture) ExecuteInvokeCallback() {
	if s.invokeCallback != nil {
		if s.executeCallbackOnlyOnce.CAS(false, true) {
			s.invokeCallback(s)
		}
	}
}

func (s *ResponseFuture) WaitResponse(timeoutMillis int64) (*protocol.RemotingCommand, error) {
	select {
	case <-s.done:
		return s.responseCommand, nil
	case <-time.After(time.Duration(timeoutMillis) * time.Millisecond):
		return nil, NewRemotingTimeoutError(s.processChannel.RemoteAddr().String(), timeoutMillis)
	}
}

func (s *ResponseFuture) PutResponse(responseCommand *protocol.RemotingCommand) {
	s.responseCommand = responseCommand
	s.done <- true
}

func (s *ResponseFuture) Release() {
	if s.once != nil {
		s.once.Release()
	}
}

func (s *ResponseFuture) GetBeginTimestamp() int64 {
	return s.beginTimestamp
}

func (s *ResponseFuture) GetTimeoutMillis() int64 {
	return s.timeoutMillis
}

func (s *ResponseFuture) GetInvokeCallback() InvokeCallback {
	return s.invokeCallback
}

func (s *ResponseFuture) IsTimeout() bool {
	diff := helper.CurrentTimeMillis() - s.beginTimestamp
	return diff > s.timeoutMillis
}

func (s *ResponseFuture) GetCause() error {
	return s.cause
}

func (s *ResponseFuture) SetCause(err error) {
	s.cause = err
}

func (s *ResponseFuture) IsSendRequestOK() bool {
	return s.sendRequestOK
}

func (s *ResponseFuture) SetSendRequestOK(sendRequestOK bool) {
	s.sendRequestOK = sendRequestOK
}

func (s *ResponseFuture) GetResponseCommand() *protocol.RemotingCommand {
	return s.responseCommand
}

func (s *ResponseFuture) SetResponseCommand(responseCommand *protocol.RemotingCommand) {
	s.responseCommand = responseCommand
}

func (s *ResponseFuture) GetOpaque() int32 {
	return s.opaque
}

func (s *ResponseFuture) GetProcessChannel() *Channel {
	return s.processChannel
}
