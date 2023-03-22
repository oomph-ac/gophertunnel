package packet

import (
	"github.com/sandertv/gophertunnel/minecraft/nbt"
	"github.com/sandertv/gophertunnel/minecraft/protocol"
)

// EditorNetwork is a packet sent from the server to the client and vise-versa to communicate editor-mode related
// information. It carries a single compound tag containing the relevant information.
type EditorNetwork struct {
	// Payload is a network little endian compound tag holding data relevant to the editor.
	Payload map[string]any
}

// ID ...
func (*EditorNetwork) ID() uint32 {
	return IDEditorNetwork
}

// Marshal ...
func (pk *EditorNetwork) Marshal(w *protocol.Writer) {
	pk.marshal(w)
}

// Unmarshal ...
func (pk *EditorNetwork) Unmarshal(r *protocol.Reader) {
	pk.marshal(r)
}

func (pk *EditorNetwork) marshal(r protocol.IO) {
	r.NBT(&pk.Payload, nbt.NetworkLittleEndian)
}
