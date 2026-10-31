package store

func incidentNativeDescriptors(backend incidentWatchBackend) []int {
	return []int{backend.(*incidentInotify).fd}
}
