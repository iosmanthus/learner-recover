package common

func IsLabelsMatch(labels map[string]string, match map[string]string) bool {
	for k, v := range labels {
		if get, ok := match[k]; !ok || get != v {
			return false
		}
	}
	return true
}
