package protocol

//MsInfoAction ...
type MsInfoAction struct {
	Name  string `json:"name"`
	Cache bool   `json:"cache"`
}

//MsInfoEvent ...
type MsInfoEvent struct {
	Name string `json:"name"`
}

//MsInfoService ...
type MsInfoService struct {
	Name     string                  `json:"name"`
	Settings interface{}             `json:"settings"`
	NodeID   string                  `json:"nodeID"`
	Actions  map[string]MsInfoAction `json:"actions"`
	Events   map[string]MsInfoEvent  `json:"events"`
}

//MsInfoClient ...
type MsInfoClient struct {
	Type        string `json:"type"`
	Version     string `json:"version"`
	LangVersion string `json:"langVersion"`
}

//MsInfoNode ...
type MsInfoNode struct {
	Ver      string          `json:"ver"`
	Sender   string          `json:"sender"`
	Services []MsInfoService `json:"services"`
	IPList   []string        `json:"ipList"`
	Client   MsInfoClient    `json:"client"`
	Port     uint            `json:"port"`
	Config   interface{}     `json:"config"`
}
