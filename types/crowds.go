package types

import (
	"fmt"
)

// -----------------------------------------------------------------------------
// CrowdsMessage

// NewEmpty implements types.Message.
func (c CrowdsMessage) NewEmpty() Message {
	return &CrowdsMessage{}
}

// Name implements types.Message.
func (c CrowdsMessage) Name() string {
	return "CrowdsMessage"
}

// String implements types.Message.
func (c CrowdsMessage) String() string {
	return fmt.Sprintf("crowds message for %s", c.Recipients)
}

// HTML implements types.Message.
func (c CrowdsMessage) HTML() string {
	return fmt.Sprintf("crowds message for %s", c.Recipients)
}

// -----------------------------------------------------------------------------
// CrowdsMessagingRequestMessage

// NewEmpty implements types.Message.
func (c CrowdsMessagingRequestMessage) NewEmpty() Message {
	return &CrowdsMessagingRequestMessage{}
}

// Name implements types.Message.
func (c CrowdsMessagingRequestMessage) Name() string {
	return "CrowdsMessagingRequestMessage"
}

// String implements types.Message.
func (c CrowdsMessagingRequestMessage) String() string {
	return fmt.Sprintf("crowds messaging request message for %s", c.FinalDst)
}

// HTML implements types.Message.
func (c CrowdsMessagingRequestMessage) HTML() string {
	return fmt.Sprintf("crowds messaging request message for %s", c.FinalDst)
}

// -----------------------------------------------------------------------------
// CrowdsDownloadRequestMessage

// NewEmpty implements types.Message.
func (c CrowdsDownloadRequestMessage) NewEmpty() Message {
	return &CrowdsDownloadRequestMessage{}
}

// Name implements types.Message.
func (c CrowdsDownloadRequestMessage) Name() string {
	return "CrowdsDownloadRequestMessage"
}

// String implements types.Message.
func (c CrowdsDownloadRequestMessage) String() string {
	return fmt.Sprintf("crowdsdownloadrequest{key:%s, id:%s}", c.Key[:8], c.RequestID)
}

// HTML implements types.Message.
func (c CrowdsDownloadRequestMessage) HTML() string {
	return fmt.Sprintf("crowdsdownloadrequest{key:%s, id:%s}", c.Key[:8], c.RequestID)
}

// -----------------------------------------------------------------------------
// CrowdsDownloadReplyMessage

// NewEmpty implements types.Message.
func (c CrowdsDownloadReplyMessage) NewEmpty() Message {
	return &CrowdsDownloadReplyMessage{}
}

// Name implements types.Message.
func (c CrowdsDownloadReplyMessage) Name() string {
	return "CrowdsDownloadReplyMessage"
}

// String implements types.Message.
func (c CrowdsDownloadReplyMessage) String() string {
	return fmt.Sprintf("crowdsdownloadrequest{key:%s, id:%s}", c.Key[:8], c.RequestID)
}

// HTML implements types.Message.
func (c CrowdsDownloadReplyMessage) HTML() string {
	return fmt.Sprintf("crowdsdownloadrequest{key:%s, id:%s}", c.Key[:8], c.RequestID)
}
