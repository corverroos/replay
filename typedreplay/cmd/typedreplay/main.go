package main

import (
	"flag"
	"fmt"
	"os"
	"path"

	"github.com/corverroos/replay/typedreplay/internal"
)

var debug = flag.Bool("debug", false, "Enables debugging compile errors")

func main() {
	flag.Parse()
	if err := run(*debug); err != nil {
		fmt.Printf("%+v", err)
		os.Exit(1)
	}
}

func run(debug bool) error {
	pwd, err := os.Getwd()
	if err != nil {
		return err
	}

	file := os.Getenv("GOFILE")
	fmt.Printf("Parsing %s...\n", file)

	ns, err := internal.Parse(path.Join(pwd, file))
	if err != nil {
		return err
	}

	if err := internal.Validate(ns); err != nil {
		return err
	}

	filename := path.Join(pwd, "replay_gen.go")

	fmt.Printf("Generating replay_gen.go with a typed API for namespace %s with %d workflows", ns.Name, len(ns.Workflows))
	src, err := internal.Render(ns, filename, debug)
	if err != nil {
		return err
	}

	return os.WriteFile(filename, src, 0644)
}
