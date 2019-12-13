package header

type CommandCustomHeader interface {
	CheckFields() error
}

type GetKVConfigRequestHeader struct {
	Namespace string
	Key       string
}

func (s *GetKVConfigRequestHeader) CheckFields() error {
	return nil
}
