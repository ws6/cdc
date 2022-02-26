package specs

//ref https://confluence.illumina.com/display/FBS/Event+Message+Specs+Design
type EventMessage struct {
	EventId      string
	DateCreated  string
	EventType    string
	ResourceType string
	ResourceId   string
	ResourceKey  string
	FieldChanges map[string]*Change
	MetaData     map[string]interface{} `json:",omitempty"`
}
type Change struct {
	NewValue string
	OldValue string `json:",omitempty"`
}
