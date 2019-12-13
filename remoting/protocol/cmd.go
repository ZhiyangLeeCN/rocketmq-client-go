package protocol

import (
	"bytes"
	"reflect"
	"strings"

	"encoding/binary"

	"github.com/ZhiyangLeeCN/rocketmq-client-go/helper"
	"github.com/ZhiyangLeeCN/rocketmq-client-go/helper/atomic"
	"github.com/ZhiyangLeeCN/rocketmq-client-go/remoting/protocol/header"
	"github.com/ZhiyangLeeCN/rocketmq-client-go/remoting/protocol/response"
)

const (
	RPC_TYPE   = 0
	RPC_ONEWAY = 1

	RESPONSE_COMMAND = 1
	REQUEST_COMMAND  = 0

	SERIALIZE_TYPE_JSON     = SerializeType("JSON")
	SERIALIZE_TYPE_ROCKETMQ = SerializeType("ROCKETMQ")

	LANGUAGE_JAVA   LanguageCode = 0
	LANGUAGE_CPP                 = 1
	LANGUAGE_DOTNET              = 2
	LANGUAGE_PYTHON              = 3
	LANGUAGE_DELPHI              = 4
	LANGUAGE_ERLANG              = 5
	LANGUAGE_RUBY                = 6
	LANGUAGE_OTHER               = 7
	LANGUAGE_HTTP                = 8
	LANGUAGE_GO                  = 9
	LANGUAGE_PHP                 = 10
	LANGUAGE_OMS                 = 11
)

type RemotingCommand struct {
	//header
	Code                    int                        `json:"code"`
	Language                string                     `json:"language"`
	Version                 int                        `json:"version"`
	Opaque                  int32                      `json:"opaque"`
	Flag                    int                        `json:"flag"`
	Remark                  string                     `json:"remark"`
	SerializeTypeCurrentRPC SerializeType              `json:"serializeTypeCurrentRPC"`
	ExtFields               map[string]interface{}     `json:"extFields"`
	CustomHeader            header.CommandCustomHeader `json:"-"`
	//body
	Body []byte `json:"-"`
}

type SerializeType string
type LanguageCode byte

var serializeTypeConfigInThisServer = SERIALIZE_TYPE_JSON
var configVersion = -1
var opaqueId = atomic.MakeInt32(0)

func SetCmdSerializeType(serializeType SerializeType) {
	serializeTypeConfigInThisServer = serializeType
}

func SetCmdVersion(version int) {
	configVersion = version
}

func (c *RemotingCommand) MarkResponseType() {
	bits := 1 << RPC_TYPE
	c.Flag = bits
}

func (c *RemotingCommand) MarkOnewayRPC() {
	bits := 1 << RPC_ONEWAY
	c.Flag |= bits
}

func (c *RemotingCommand) IsOnewayRPC() bool {
	bits := 1 << RPC_ONEWAY
	return (c.Flag & bits) == bits
}

func (c *RemotingCommand) IsResponseType() bool {
	bits := 1 << RPC_TYPE
	return (c.Flag & bits) == bits
}

func (c *RemotingCommand) GetType() byte {
	if c.IsResponseType() {
		return RESPONSE_COMMAND
	}

	return REQUEST_COMMAND
}

func (c *RemotingCommand) makeCustomHeaderToNet() {
	if c.CustomHeader != nil {
		c.ExtFields = make(map[string]interface{})
		v := reflect.ValueOf(c.CustomHeader)
		s := v.Elem()
		typeOfT := s.Type()
		for i := 0; i < s.NumField(); i++ {
			f := s.Field(i)
			name := typeOfT.Field(i).Name
			if !strings.HasPrefix(name, "this") {
				name = helper.UnCapitalizeStr(name)
				value := f.Interface()
				if value != nil {
					c.ExtFields[name] = value
				}
			}
		}
	}
}

func (c *RemotingCommand) headerEncode() []byte {
	c.makeCustomHeaderToNet()
	if c.SerializeTypeCurrentRPC == SERIALIZE_TYPE_JSON {
		return JsonEncode(c)
	} else {
		//TODO: RocketMQSerialize
		return nil
	}
}

func (c *RemotingCommand) EncodeHeaderWithBodyLength() *bytes.Buffer {
	bodyLength := 0
	if c.Body != nil {
		bodyLength = len(c.Body)
	}
	return c.EncodeHeader(int32(bodyLength))
}

func (c *RemotingCommand) EncodeHeader(bodyLength int32) *bytes.Buffer {
	// 1> header length size
	length := int32(4)

	// 2> header data length
	headerData := c.headerEncode()
	headerDataLen := int32(len(headerData))
	length += headerDataLen

	// 3> body data length
	length += bodyLength

	buf := bytes.NewBuffer(make([]byte, 0, 4+length-bodyLength))

	// length
	binary.Write(buf, binary.BigEndian, length)

	// header length
	binary.Write(buf, binary.BigEndian, MarkProtocolType(headerDataLen, c.SerializeTypeCurrentRPC))

	// header data
	buf.Write(headerData)

	return buf
}

func (c *RemotingCommand) DecodeCommandCustomHeader(v interface{}) error {
	if c.ExtFields != nil {
		v := reflect.ValueOf(v)
		s := v.Elem()
		typeOfT := s.Type()
		for i := 0; i < s.NumField(); i++ {
			f := s.Field(i)
			name := typeOfT.Field(i).Name
			if !strings.HasPrefix(name, "this") {
				name = helper.UnCapitalizeStr(name)
				value := c.ExtFields[name]
				if value != nil {
					setValue := reflect.ValueOf(value).Convert(f.Type())
					f.Set(setValue)
				}
			}
		}
	}

	return nil
}

func setCmdVersion(cmd *RemotingCommand) {
	if configVersion > 0 {
		cmd.Version = configVersion
	}
}

func headerDecode(headerData []byte, serializeType SerializeType) *RemotingCommand {
	if serializeType == SERIALIZE_TYPE_JSON {
		cmd := &RemotingCommand{}
		err := JsonDecode(headerData, cmd)
		if err != nil {
			return nil
		} else {
			return cmd
		}
	} else {
		return nil
	}
}

func DecodeRemoteCommand(byteBuffer *bytes.Buffer) *RemotingCommand {
	var length, oriHeaderLen, headerLength int32

	length = int32(byteBuffer.Len())
	binary.Read(byteBuffer, binary.BigEndian, &oriHeaderLen)
	headerLength = GetProtocolHeaderLength(oriHeaderLen)

	headerData := make([]byte, headerLength)
	byteBuffer.Read(headerData)

	cmd := headerDecode(headerData, GetProtocolType(oriHeaderLen))
	if cmd != nil {
		bodyLength := length - 4 - headerLength
		if bodyLength > 0 {
			bodyData := make([]byte, bodyLength)
			byteBuffer.Read(bodyData)
			cmd.Body = bodyData
		} else {
			cmd.Body = nil
		}

		return cmd
	} else {
		return nil
	}
}

func CreateCmdOpaqueId() int32 {
	return opaqueId.Inc()
}

func CreateCmdRequestId() int32 {
	return CreateCmdOpaqueId()
}

func CreateCmdResponseId() int32 {
	return CreateCmdOpaqueId()
}

func CreateRequestCommand(code int, customHeader header.CommandCustomHeader) *RemotingCommand {
	cmd := &RemotingCommand{
		Code:                    code,
		Opaque:                  CreateCmdRequestId(),
		Language:                LanguageNameOf(LANGUAGE_GO),
		CustomHeader:            customHeader,
		SerializeTypeCurrentRPC: serializeTypeConfigInThisServer,
	}
	setCmdVersion(cmd)
	return cmd
}

func CreateResponseCommand(code int, remark string, header header.CommandCustomHeader) *RemotingCommand {
	cmd := &RemotingCommand{
		Code:                    code,
		Opaque:                  CreateCmdResponseId(),
		Language:                LanguageNameOf(LANGUAGE_GO),
		Remark:                  remark,
		CustomHeader:            header,
		SerializeTypeCurrentRPC: serializeTypeConfigInThisServer,
	}
	cmd.MarkResponseType()
	setCmdVersion(cmd)
	return cmd
}

func CreateRemarkResponseCommand(code int, remark string) *RemotingCommand {
	return CreateResponseCommand(code, remark, nil)
}

func CreateHeaderResponseCommand(header header.CommandCustomHeader) *RemotingCommand {
	return CreateResponseCommand(response.SYSTEM_ERROR, "not set any response code", header)
}
