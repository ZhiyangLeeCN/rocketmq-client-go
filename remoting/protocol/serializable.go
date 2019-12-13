package protocol

type RemotingSerializable struct{}

func (s *RemotingSerializable) Decode(data []byte) error {
	err := JsonDecode(data, s)
	if err != nil {
		return err
	}
	return nil
}

func (s *RemotingSerializable) Encode() []byte {
	return JsonEncode(s)
}
