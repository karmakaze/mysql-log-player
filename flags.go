package main

import (
	"flag"
	"os"

	logger "github.com/500px/go-utils/chatty_logger"
)

var (
	logLevel     = logger.LogSeverity("log", logger.INFO, "The log level to emit.")
	printVersion = flag.Bool("version", false, "Print version information and exit.")

	sourcePath  = flag.String("source", "", "Path of input log or empty for stdin.")
	workerCount = flag.Int("workers", 5, "Number of query workers.")
)

func parseFlags() {
	flag.Parse()
	logger.Init(*logLevel)

	if *printVersion {
		version()
		os.Exit(0)
	}
}
