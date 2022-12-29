package types

import "fmt"

func (c ConfidentialityMessage) NewEmpty() Message {
	return &ConfidentialityMessage{}
}

func (c ConfidentialityMessage) Name() string {
	return "confidentiality"
}

func (c ConfidentialityMessage) String() string {
	return fmt.Sprintf("{confidentiality cipher - %s}", c.CipherMessage)
}

func (c ConfidentialityMessage) HTML() string {
	return c.String()
}
