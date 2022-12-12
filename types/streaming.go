package types

import "fmt"

// -----------------------------------------------------------------------------
// StreamConnectMessage

// NewEmpty implements types.Message.
func (sc StreamConnectMessage) NewEmpty() Message {
	return &StreamConnectMessage{}
}

// Name implements types.Message.
func (sc StreamConnectMessage) Name() string {
	return "streamconnectmessage"
}

// String implements types.Message.
func (sc StreamConnectMessage) String() string {
	return fmt.Sprintf("StreamConnectMessage{streamID:%s, clientID:%s, streamerID: %s}",
		sc.StreamID[:8], sc.ClientID, sc.StreamerID)
}

// HTML implements types.Message.
func (sc StreamConnectMessage) HTML() string {
	return sc.String()
}

// -----------------------------------------------------------------------------
// StreamDisconnectMessage

// NewEmpty implements types.Message.
func (sc StreamDisconnectMessage) NewEmpty() Message {
	return &StreamDisconnectMessage{}
}

// Name implements types.Message.
func (sc StreamDisconnectMessage) Name() string {
	return "streamdisconnectmessage"
}

// String implements types.Message.
func (sc StreamDisconnectMessage) String() string {
	return fmt.Sprintf("StreamDisconnectMessage{streamID:%s, clientID:%s, streamerID: %s}",
		sc.StreamID[:8], sc.ClientID, sc.StreamerID)
}

// HTML implements types.Message.
func (sc StreamDisconnectMessage) HTML() string {
	return sc.String()
}

// -----------------------------------------------------------------------------
// StreamStartMessage

// NewEmpty implements types.Message.
func (sc StreamStartMessage) NewEmpty() Message {
	return &StreamStartMessage{}
}

// Name implements types.Message.
func (sc StreamStartMessage) Name() string {
	return "streamstartmessage"
}

// String implements types.Message.
func (sc StreamStartMessage) String() string {
	return fmt.Sprintf("StreamStartMessage{streamID:%s, streamerID: %s}",
		sc.StreamID[:8], sc.StreamerID)
}

// HTML implements types.Message.
func (sc StreamStartMessage) HTML() string {
	return sc.String()
}

// -----------------------------------------------------------------------------
// StreamMessage

// NewEmpty implements types.Message.
func (sc StreamMessage) NewEmpty() Message {
	return &StreamMessage{}
}

// Name implements types.Message.
func (sc StreamMessage) Name() string {
	return "streammessage"
}

// String implements types.Message.
func (sc StreamMessage) String() string {
	return fmt.Sprintf("StreamMessage{streamID:%s, name:%s, price:%d,watching: %d}",
		sc.StreamInfo.StreamID[:8], sc.StreamInfo.Name, sc.StreamInfo.Price, sc.StreamInfo.CurrentlyWatching)
}

// HTML implements types.Message.
func (sc StreamMessage) HTML() string {
	return sc.String()
}

// -----------------------------------------------------------------------------
// StreamDataMessage

// NewEmpty implements types.Message.
func (sc StreamDataMessage) NewEmpty() Message {
	return &StreamDataMessage{}
}

// Name implements types.Message.
func (sc StreamDataMessage) Name() string {
	return "streamdatamessage"
}

// String implements types.Message.
func (sc StreamDataMessage) String() string {
	return fmt.Sprintf("StreamDataMessage{streamID:%s}",
		sc.ID[:8])
}

// HTML implements types.Message.
func (sc StreamDataMessage) HTML() string {
	return sc.String()
}

// -----------------------------------------------------------------------------
// StreamAcceptedMessage

// NewEmpty implements types.Message.
func (sc StreamAcceptMessage) NewEmpty() Message {
	return &StreamAcceptMessage{}
}

// Name implements types.Message.
func (sc StreamAcceptMessage) Name() string {
	return "streamacceptmessage"
}

// String implements types.Message.
func (sc StreamAcceptMessage) String() string {
	return fmt.Sprintf("StreamAcceptedMessage{streamID:%s, streamer:%s, client:%s}",
		sc.StreamID[:8], sc.StreamerID, sc.ClientID)
}

// HTML implements types.Message.
func (sc StreamAcceptMessage) HTML() string {
	return sc.String()
}
