package main

import (
	"os"

	"example.com/order-pipeline/internal/app"
)

func main() {
	os.Exit(app.Run(os.Stdin, os.Stdout, os.Stderr, os.Args[1:]))
}
