package common

type RecoverInfo struct {
	StoreIDs  []uint64 `json:"storeIDs"`
	ClusterID string   `json:"clusterID"`
	AllocID   uint64   `json:"allocID"`
}

func (i *RecoverInfo) IsEmpty() bool {
	return len(i.StoreIDs) == 0 && i.ClusterID == "" && i.AllocID == 0
}
