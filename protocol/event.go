package protocol

//MsEvent ...
type MsEvent struct {
	Ver    string   `json:"ver"`
	Sender string   `json:"sender"`
	Event  string   `json:"event"`
	Data   string   `json:"data"`
	Groups []string `json:"groups"`
}
