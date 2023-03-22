package packet

import (
	"github.com/sandertv/gophertunnel/minecraft/protocol"
)

// ScriptMessage is used to communicate custom messages from the client to the server, or from the server to the client.
// While the name may suggest this packet is used for the discontinued scripting API, it is likely instead for the
// GameTest framework.
type ScriptMessage struct {
	// Identifier is the identifier of the message, used by either party to identify the message data sent.
	Identifier string
	// Data contains the data of the message.
	Data []byte
}

// ID ...
func (pk *ScriptMessage) ID() uint32 {
	return IDScriptMessage
}

// Marshal ...
func (pk *ScriptMessage) Marshal(w *protocol.Writer) {
	pk.marshal(w)
}

// Unmarshal ...
func (pk *ScriptMessage) Unmarshal(r *protocol.Reader) {
	pk.marshal(r)
}

func (pk *ScriptMessage) marshal(r protocol.IO) {
	r.String(&pk.Identifier)
	r.ByteSlice(&pk.Data)
}
