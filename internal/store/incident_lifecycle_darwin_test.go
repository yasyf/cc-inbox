package store

func incidentNativeDescriptors(backend incidentWatchBackend) []int {
	raw := backend.(*incidentKqueue)
	descriptors := make([]int, 0, len(raw.files)+1)
	descriptors = append(descriptors, raw.fd)
	for _, fd := range raw.files {
		descriptors = append(descriptors, fd)
	}
	return descriptors
}
