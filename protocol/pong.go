package protocol

//MsPong ...
type MsPong struct {
	Ver     string `json:"ver"`
	Sender  string `json:"sender"`
	Time    int64  `json:"time"`
	Arrived int64  `json:"arrived"`
}
