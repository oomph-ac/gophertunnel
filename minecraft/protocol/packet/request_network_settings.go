package packet

import (
	"github.com/sandertv/gophertunnel/minecraft/protocol"
)

// RequestNetworkSettings is sent by the client to request network settings, such as compression, from the server.
type RequestNetworkSettings struct {
	// ClientProtocol is the protocol version of the player. The player is disconnected if the protocol is
	// incompatible with the protocol of the server.
	ClientProtocol int32
}

// ID ...
func (pk *RequestNetworkSettings) ID() uint32 {
	return IDRequestNetworkSettings
}

// Marshal ...
func (pk *RequestNetworkSettings) Marshal(w *protocol.Writer) {
	pk.marshal(w)
}

// Unmarshal ...
func (pk *RequestNetworkSettings) Unmarshal(r *protocol.Reader) {
	pk.marshal(r)
}

func (pk *RequestNetworkSettings) marshal(r protocol.IO) {
	r.BEInt32(&pk.ClientProtocol)
}
