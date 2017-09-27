package protocol

//MsPing ...
type MsPing struct {
	Ver    string `json:"ver"`
	Sender string `json:"sender"`
	Time   uint64 `json:"time"`
}
