package packet

import (
	"github.com/sandertv/gophertunnel/minecraft/protocol"
)

// DeathInfo is a packet sent from the server to the client expected to be sent when a player dies. It contains messages
// related to the player's death, which are shown on the death screen as of v1.19.10.
type DeathInfo struct {
	// Cause is the cause of the player's death, such as "suffocation" or "suicide".
	Cause string
	// Messages is a list of death messages to be shown on the death screen.
	Messages []string
}

// ID ...
func (*DeathInfo) ID() uint32 {
	return IDDeathInfo
}

// Marshal ...
func (pk *DeathInfo) Marshal(w *protocol.Writer) {
	pk.marshal(w)
}

// Unmarshal ...
func (pk *DeathInfo) Unmarshal(r *protocol.Reader) {
	pk.marshal(r)
}

func (pk *DeathInfo) marshal(r protocol.IO) {
	r.String(&pk.Cause)
	protocol.FuncSlice(r, &pk.Messages, r.String)
}
