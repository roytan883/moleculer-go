package protocol

//MsPong ...
type MsPong struct {
	Ver     string `json:"ver"`
	Sender  string `json:"sender"`
	Time    uint64 `json:"time"`
	Arrived uint64 `json:"arrived"`
}
