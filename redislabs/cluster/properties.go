package cluster

// InstanceCredentials contains properties necessary for identifying a
// cluster instance (database) and connecting to it.
type InstanceCredentials struct {
	UID      int
	Host     string
	Port     int
	IPList   []string
	Password string
}
