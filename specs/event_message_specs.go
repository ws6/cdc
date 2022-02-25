package specs

//ref https://confluence.illumina.com/display/FBS/Event+Message+Specs+Design
type EventMessage struct {
	EventId      string `json:",omitempty"`
	DateCreated  string `json:",omitempty"`
	EventType    string `json:",omitempty"`
	ResourceType string `json:",omitempty"`
	ResourceId   string `json:",omitempty"`
	ResourceKey  string `json:",omitempty"`
	FieldChanges map[string]*Change
	MetaData     map[string]interface{} `json:",omitempty"`
}
type Change struct {
	NewValue string
	OldValue string `json:",omitempty"`
}
