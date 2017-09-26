package protocol

//MsHeartbeat ...
type MsHeartbeat struct {
	Ver    string  `json:"ver"`
	Sender string  `json:"sender"`
	CPU    float32 `json:"cpu"`
}
