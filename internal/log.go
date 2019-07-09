package internal

import (
	"log"
	"os"
)

type Logger interface {
	Printf(format string, v ...interface{})
}

var Log Logger = log.New(os.Stderr, "cache: ", log.LstdFlags|log.Lshortfile)
