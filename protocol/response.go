package protocol

//MsResponse ...
type MsResponse struct {
	Ver     string      `json:"ver"`
	Sender  string      `json:"sender"`
	ID      string      `json:"id"`
	Success bool        `json:"success"`
	Data    interface{} `json:"data"`
	Error   interface{} `json:"error"`
	Meta    interface{} `json:"meta"`
}
