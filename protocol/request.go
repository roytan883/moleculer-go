package protocol

//MsRequest ...
type MsRequest struct {
	Ver       string      `json:"ver"`
	Sender    string      `json:"sender"`
	ID        string      `json:"id"`
	Action    string      `json:"action"`
	Params    interface{} `json:"params"`
	Meta      interface{} `json:"meta"`
	Timeout   uint32      `json:"timeout"`
	Level     uint32      `json:"level"`
	Metrics   bool        `json:"metrics"`
	ParentID  string      `json:"parentID"`
	RequestID string      `json:"requestID"`
}
