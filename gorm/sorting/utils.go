package sorting

import "regexp"

var fieldNameRegexp = regexp.MustCompile(`^[a-zA-Z0-9_\.]+$`)

func toDirection(desc bool) string {
	if desc {
		return "DESC"
	} else {
		return "ASC"
	}
}
