package specs

//ref https://confluence.illumina.com/display/FBS/Event+Message+Specs+Design
type EventMessage struct {
	EventId      string
	DateCreated  string
	EventType    string
	ResourceType string
	ResoureId    string
	FieldChanges map[string]*Change
	MetaData     map[string]interface{}
}
type Change struct {
	NewValue string
	OldValue string
}
