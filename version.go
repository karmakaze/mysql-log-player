package main

import (
	"fmt"
)

var (
	Version = map[string]string{
		"GoVersion":   "n/a",
		"CommitHash":  "n/a",
		"BranchName":  "n/a",
		"BuildNumber": "n/a",
		"BuiltBy":     "n/a",
		"BuiltOn":     "n/a",
	}
)

func version() {
	fmt.Printf("Version Information:\n")
	fmt.Printf("  Go Version:   %s\n", Version["GoVersion"])
	fmt.Printf("  Commit:       %s\n", Version["CommitHash"])
	fmt.Printf("  Branch:       %s\n", Version["BranchName"])
	fmt.Printf("  Build Number: %s\n", Version["BuildNumber"])
	fmt.Printf("  Built By:     %s\n", Version["BuiltBy"])
	fmt.Printf("  Built On:     %s\n", Version["BuiltOn"])
}
