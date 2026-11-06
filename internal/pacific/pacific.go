package pacific

import (
	"time"
	_ "time/tzdata"
)

var Zone = func() *time.Location {
	zone, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		panic(err)
	}
	return zone
}()
