package types

import "fmt"

// -----------------------------------------------------------------------------
// MulticastMessage

// NewEmpty implements types.Message.
func (sc MulticastMessage) NewEmpty() Message {
	return &MulticastMessage{}
}

// Name implements types.Message.
func (sc MulticastMessage) Name() string {
	return "multicastmessage"
}

// String implements types.Message.
func (sc MulticastMessage) String() string {
	return fmt.Sprintf("MulticastMessage{streamID:%s}",
		sc.ID[:8])
}

// HTML implements types.Message.
func (sc MulticastMessage) HTML() string {
	return sc.String()
}

// -----------------------------------------------------------------------------
// MulticastJoinMessage

// NewEmpty implements types.Message.
func (sc MulticastJoinMessage) NewEmpty() Message {
	return &MulticastJoinMessage{}
}

// Name implements types.Message.
func (sc MulticastJoinMessage) Name() string {
	return "multicastjoinmessage"
}

// String implements types.Message.
func (sc MulticastJoinMessage) String() string {
	return fmt.Sprintf("MulticastJoinMessage{streamID:%s, streamer:%s, client:%s}",
		sc.ID[:8], sc.StreamerID, sc.ClientID)
}

// HTML implements types.Message.
func (sc MulticastJoinMessage) HTML() string {
	return sc.String()
}

// -----------------------------------------------------------------------------
// MulticastStopMessage

// NewEmpty implements types.Message.
func (sc MulticastStopMessage) NewEmpty() Message {
	return &MulticastStopMessage{}
}

// Name implements types.Message.
func (sc MulticastStopMessage) Name() string {
	return "multicaststopmessage"
}

// String implements types.Message.
func (sc MulticastStopMessage) String() string {
	return fmt.Sprintf("MulticastStopMessage{streamID:%s}",
		sc.ID[:8])
}

// HTML implements types.Message.
func (sc MulticastStopMessage) HTML() string {
	return sc.String()
}
