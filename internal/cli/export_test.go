package cli

import "time"

func SetPostPatience(d time.Duration) func() {
	was := postPatience
	postPatience = d
	return func() { postPatience = was }
}
