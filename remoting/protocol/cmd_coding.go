package protocol

import (
	"encoding/json"
)

func MarkProtocolType(source int32, serializeType SerializeType) []byte {
	result := make([]byte, 4)

	result[0] = SerializeTypeOf(serializeType)
	result[1] = (byte)((source >> 16) & 0xFF)
	result[2] = (byte)((source >> 8) & 0xFF)
	result[3] = (byte)(source & 0xFF)
	return result
}

func GetProtocolHeaderLength(length int32) int32 {
	return length & 0xFFFFFF
}

func GetProtocolType(source int32) SerializeType {
	return SerializeNameOf((byte)(source>>24) & 0xFF)
}

func SerializeTypeOf(serializeType SerializeType) byte {
	switch serializeType {
	case SERIALIZE_TYPE_JSON:
		return 0
	case SERIALIZE_TYPE_ROCKETMQ:
		return 1
	default:
		return 0
	}
}

func SerializeNameOf(serializeType byte) SerializeType {
	switch serializeType {
	case 0:
		return SERIALIZE_TYPE_JSON
	case 1:
		return SERIALIZE_TYPE_ROCKETMQ
	default:
		return SERIALIZE_TYPE_JSON
	}
}

func LanguageNameOf(i LanguageCode) string {
	switch i {
	case LANGUAGE_JAVA:
		return "JAVA"
	case LANGUAGE_CPP:
		return "CPP"
	case LANGUAGE_DOTNET:
		return "DOTNET"
	case LANGUAGE_PYTHON:
		return "PYTHON"
	case LANGUAGE_DELPHI:
		return "DELPHI"
	case LANGUAGE_ERLANG:
		return "ERLANG"
	case LANGUAGE_RUBY:
		return "RUBY"
	case LANGUAGE_HTTP:
		return "HTTP"
	case LANGUAGE_GO:
		return "GO"
	case LANGUAGE_PHP:
		return "PHP"
	case LANGUAGE_OMS:
		return "OMS"
	default:
		return "OTHER"
	}
}

func JsonEncode(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return b
}

func JsonDecode(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func BodyEncode(v interface{}) []byte {
	return JsonEncode(v)
}

func BodyDecode(data []byte, v interface{}) error {
	return JsonDecode(data, v)
}
