package remoting

import (
	"bytes"
	"fmt"
	"github.com/onatm/clockwerk"
	"io"
	"sync"
	"time"

	"encoding/binary"

	"github.com/ZhiyangLeeCN/rocketmq-client-go/helper"
	"github.com/ZhiyangLeeCN/rocketmq-client-go/helper/atomic"
	"github.com/ZhiyangLeeCN/rocketmq-client-go/remoting/protocol"
	"github.com/ZhiyangLeeCN/rocketmq-client-go/remoting/protocol/response"
)

type RemotingClient struct {
	channelTable     map[string]*Channel
	channelTableLock *sync.RWMutex
	responseTable    *sync.Map
	processorTable   map[int]RequestProcessor
	semaphoreOneway  *atomic.Semaphore
	semaphoreAsync   *atomic.Semaphore

	scheduler  *clockwerk.Clockwerk
	isShutdown chan bool
}

func NewRemotingClient(remotingConfig *RemotingConfig) *RemotingClient {
	return &RemotingClient{
		channelTable:     make(map[string]*Channel),
		channelTableLock: &sync.RWMutex{},
		responseTable:    &sync.Map{},
		processorTable:   make(map[int]RequestProcessor),
		semaphoreAsync:   atomic.NewSemaphore(remotingConfig.ClientAsyncSemaphoreValue),
		semaphoreOneway:  atomic.NewSemaphore(remotingConfig.ClientOnewaySemaphoreValue),
	}
}

func (s *RemotingClient) Start() error {

	s.scheduler = clockwerk.New()
	s.scheduler.Every(3 * time.Second).Do(helper.NewRunnable(func() {
		s.scanResponseTable()
	}))
	s.scheduler.Start()

	return nil
}

func (s *RemotingClient) Shutdown() error {

	s.scheduler.Stop()

	for _, channel := range s.channelTable {
		s.closeChannel(channel)
	}

	return nil
}

func (s *RemotingClient) scanResponseTable() {

	s.responseTable.Range(func(key, value interface{}) bool {
		repFuture := value.(*ResponseFuture)
		if (repFuture.GetBeginTimestamp() + repFuture.GetTimeoutMillis() + 1000) <= helper.CurrentTimeMillis() {
			repFuture.Release()
			s.responseTable.Delete(key)
			repFuture.ExecuteInvokeCallback()
		}
		return true
	})
}

func (s *RemotingClient) GetChannelOrCreate(addr string) (*Channel, error) {
	channel := s.GetChannel(addr)
	if channel != nil {
		return channel, nil
	}

	channel, err := s.CreateChannel(addr)
	if err != nil {
		return nil, err
	}

	return channel, err
}

func (s *RemotingClient) GetChannel(addr string) *Channel {
	defer s.channelTableLock.RUnlock()
	s.channelTableLock.RLock()
	return s.channelTable[addr]
}

func (s *RemotingClient) CreateChannel(addr string) (*Channel, error) {
	defer s.channelTableLock.Unlock()
	s.channelTableLock.Lock()
	channel := s.channelTable[addr]
	if channel != nil {
		return channel, nil
	}

	channel, err := s.dialTcpConnAndStartReceiveLoop(addr)
	if err != nil {
		return nil, err
	}
	s.channelTable[channel.RemoteAddr().String()] = channel
	return channel, nil
}

func (s *RemotingClient) dialTcpConnAndStartReceiveLoop(addr string) (*Channel, error) {
	channel, err := ChannelConnect("tcp", addr)
	if err != nil {
		return nil, err
	}

	go s.receiveLoop(channel)

	return channel, nil
}

func (s *RemotingClient) receiveLoop(channel *Channel) {

	defer s.closeChannel(channel)

	for {
		var length int32

		buf := make([]byte, 4)
		_, err := io.ReadFull(channel, buf)
		if err != nil {
			break
		}

		err = binary.Read(bytes.NewBuffer(buf), binary.BigEndian, &length)
		if err != nil {
			break
		}

		cmdBuf := make([]byte, length)
		_, err = io.ReadFull(channel, cmdBuf)
		if err != nil {
			break
		}

		cmd := protocol.DecodeRemoteCommand(bytes.NewBuffer(cmdBuf))
		if cmd != nil {
			switch cmd.GetType() {
			case protocol.REQUEST_COMMAND:
				go s.processRequestCommand(channel, cmd)
				break
			case protocol.RESPONSE_COMMAND:
				go s.processResponseCommand(channel, cmd)
				break
			}
		}
	}
}

func (s *RemotingClient) processRequestCommand(channel *Channel, cmd *protocol.RemotingCommand) {
	processor := s.processorTable[cmd.Code]
	if processor != nil {
		responseCmd, err := processor.ProcessRequest(channel, cmd)
		if err != nil {
			if !cmd.IsOnewayRPC() {
				responseCmd = protocol.CreateResponseCommand(response.SYSTEM_ERROR, err.Error(), nil)
				responseCmd.Opaque = cmd.Opaque
				_ = channel.WriteCmd(responseCmd)
			}
		} else {
			if !cmd.IsOnewayRPC() {
				if responseCmd != nil {
					responseCmd.Opaque = cmd.Opaque
					responseCmd.MarkResponseType()
					_ = channel.WriteCmd(responseCmd)
				}
			}
		}
	}
}

func (s *RemotingClient) processResponseCommand(channel *Channel, responseCommand *protocol.RemotingCommand) {
	v, ok := s.responseTable.Load(responseCommand.Opaque)
	if ok {
		s.responseTable.Delete(responseCommand.Opaque)
		responseFuture := v.(*ResponseFuture)
		responseFuture.SetResponseCommand(responseCommand)
		if responseFuture.GetInvokeCallback() != nil {
			go func() {
				responseFuture.ExecuteInvokeCallback()
				responseFuture.Release()
			}()
		} else {
			responseFuture.PutResponse(responseCommand)
			responseFuture.Release()
		}
	}
}

func (s *RemotingClient) closeChannel(channel *Channel) {
	defer s.channelTableLock.Unlock()

	addr := channel.RemoteAddr().String()
	s.channelTableLock.Lock()

	prevChannel := s.channelTable[addr]
	if prevChannel != nil {
		delete(s.channelTable, addr)
		_ = prevChannel.Close()
	}
}

func (s *RemotingClient) InvokeSync(addr string, request *protocol.RemotingCommand,
	timeoutMillis int64) (*protocol.RemotingCommand, error) {
	beginStartTime := helper.CurrentTimeMillis()
	channel, err := s.GetChannelOrCreate(addr)
	if err != nil {
		return nil, err
	}
	if channel != nil && channel.IsActive() {
		costTime := helper.CurrentTimeMillis() - beginStartTime
		if timeoutMillis < costTime {
			return nil, &RemotingTimeoutError{
				"invokeSync call timeout",
			}
		}
		responseFuture := NewDefaultResponseFuture(channel, request.Opaque, timeoutMillis, nil, nil)
		s.responseTable.Store(request.Opaque, responseFuture)
		go func() {
			err := channel.WriteCmd(request)
			if err != nil {
				s.responseTable.Delete(request.Opaque)

				responseFuture.SetCause(err)
				responseFuture.SetSendRequestOK(false)
				responseFuture.PutResponse(nil)
			}
		}()

		responseCmd, err := responseFuture.WaitResponse(timeoutMillis)
		if err != nil {

			switch err.(type) {
			case *RemotingTimeoutError:
				s.closeChannel(channel)
			}

			return nil, err

		} else {

			if responseFuture.IsSendRequestOK() {
				return responseCmd, nil
			} else {
				return nil, responseFuture.GetCause()
			}

		}

	} else {
		s.closeChannel(channel)
		return nil, NewRemotingConnectError(addr)
	}
}

func (s *RemotingClient) InvokeAsync(addr string, request *protocol.RemotingCommand, timeoutMillis int64,
	invokeCallback InvokeCallback) error {
	beginStartTime := helper.CurrentTimeMillis()
	channel, err := s.GetChannelOrCreate(addr)
	if err != nil {
		return err
	}
	if channel != nil && channel.IsActive() {
		once := NewSemaphoreReleaseOnlyOnce(s.semaphoreAsync)
		if s.semaphoreAsync.TryAcquire(1, time.Duration(timeoutMillis)*time.Millisecond) {
			costTime := helper.CurrentTimeMillis() - beginStartTime
			if timeoutMillis < costTime {
				once.Release()
				return &RemotingTimeoutError{
					"invokeAsync call timeout",
				}
			}
			responseFuture := NewDefaultResponseFuture(channel, request.Opaque, timeoutMillis, invokeCallback, once)
			s.responseTable.Store(request.Opaque, responseFuture)
			go func() {
				err := channel.WriteCmd(request)
				if err != nil {
					s.responseTable.Delete(request.Opaque)

					responseFuture.SetCause(err)
					responseFuture.SetSendRequestOK(false)
					responseFuture.PutResponse(nil)
					responseFuture.ExecuteInvokeCallback()
					responseFuture.Release()
				}
			}()
			return nil
		} else {
			if timeoutMillis <= 0 {
				return &RemotingTooMuchRequestError{
					"invokeAsync invoke too fast",
				}
			} else {
				return &RemotingTimeoutError{
					fmt.Sprintf("invokeAsyncImpl tryAcquire semaphore timeout, %dms", timeoutMillis),
				}
			}
		}
	} else {
		s.closeChannel(channel)
		return NewRemotingConnectError(addr)
	}
}

func (s *RemotingClient) InvokeOneway(addr string, request *protocol.RemotingCommand,
	timeoutMillis int64) error {
	channel, err := s.GetChannelOrCreate(addr)
	if err != nil {
		return err
	}
	if channel != nil && channel.IsActive() {
		once := NewSemaphoreReleaseOnlyOnce(s.semaphoreAsync)
		if s.semaphoreAsync.TryAcquire(1, time.Duration(timeoutMillis)*time.Millisecond) {
			go func() {
				err := channel.WriteCmd(request)
				once.Release()
				if err != nil {
					//TODO:log
				}
			}()
			return nil
		} else {
			if timeoutMillis <= 0 {
				return &RemotingTooMuchRequestError{
					"invokeOneway invoke too fast",
				}
			} else {
				return &RemotingTimeoutError{
					fmt.Sprintf("invokeOneway tryAcquire semaphore timeout, %dms", timeoutMillis),
				}
			}
		}
	} else {
		s.closeChannel(channel)
		return NewRemotingConnectError(addr)
	}
}

func (s *RemotingClient) RegisterProcessor(requestCode int, processor RequestProcessor) {
	s.processorTable[requestCode] = processor
}
