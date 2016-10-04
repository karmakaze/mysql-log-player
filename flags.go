package main

import (
	"flag"
	"os"

	logger "github.com/500px/go-utils/chatty_logger"
	"github.com/500px/go-utils/flags"
	"fmt"
)

var (
	dbHost       = flag.String("db-host", "127.0.0.1", "MySQL host")
	dbName       = flag.String("db-name", "500px", "MySQL database name")
	dbUser       = flag.String("db-user", os.Getenv("DB_USER"), "MySQL username (env DB_USER)")
	dbPass       = flag.String("db-pass", "", "MySQL password (env DB_PASS)")
	logLevel     = logger.LogSeverity("log", logger.INFO, "The log level to emit.")
	printVersion = flag.Bool("version", false, "Print version information and exit.")
	format       = flag.String("format", "mysql-sniffer", "Either mysql-sniffer (ip:port:sql) or vc-mysql-sniffer (# Time:/# User@Host:/# Query_time:/sql)")
	readOnly     = flag.Bool("read-only", true, "Only execute read-only (SELECT...) statements.")
	dryRun       = flag.Bool("dry-run", false, "Only print statements that would be executed.")

	sourcePath  = flag.String("source", "", "Path of input log or empty for stdin.")
	workerCount = flag.Int("workers", 5, "Number of query workers.")
)

func parseFlags() {
	flags.SetFromEnv("db-user", "DB_USER")
	flags.SetFromEnv("db-pass", "DB_PASS")

	flag.Parse()
	logger.Init(*logLevel)

	if *printVersion {
		version()
		os.Exit(0)
	}

	if *format != "mysql-sniffer" && *format != "vc-mysql-sniffer" {
		fmt.Println("Format must be either 'mysql-sniffer' or 'vc-mysql-sniffer'")
		os.Exit(1)
	}

	if !*dryRun && (*dbUser == "" || *dbPass == "") {
		fmt.Println("Both db-user and db-pass (env DB_USER and DB_PASS) are required; for no password use \"''\"")
		os.Exit(1)
	}
	if *dbPass == "''" {
		*dbPass = ""
	}
}
