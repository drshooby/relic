package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
)

const logBottlePathWithoutHome = "Library/Application Support/CrossOver/Bottles/Steam Library/drive_c/users/crossover/AppData/Local/Warframe/EE.log"

func isValidPath(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && fi.Mode().IsRegular()
}

func buildPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("failed to resolve user home directory: %w", err)
	}
	return filepath.Join(home, logBottlePathWithoutHome), nil
}

func main() {
	pathFlag := flag.String("path", "", "path to EE.log (defaults to the CrossOver bottle location)")
	once := flag.Bool("once", false, "read what is currently in the file, emit it, and exit")
	sinkFlag := flag.String("sink", "stdout", "where to flush output to")
	flag.Parse()

	path := *pathFlag
	if path == "" {
		p, err := buildPath()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		path = p
	}
	if !isValidPath(path) {
		fmt.Fprintf(os.Stderr, "no readable log at %s\n", path)
		os.Exit(1)
	}

	var sink Sink
	if *sinkFlag == "kinesis" {
		sink = NewKinesisSink(os.Stdout)
	} else {
		sink = NewStdoutSink(os.Stdout)
	}
	tailer, err := NewTailer(path, sink)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer tailer.Close()

	var runErr error
	if *once {
		runErr = tailer.Poll()
	} else {
		stop := make(chan struct{})
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
		go func() {
			<-sigs
			close(stop)
		}()

		fmt.Fprintf(os.Stderr, "tailing %s\n", path)
		runErr = tailer.Run(stop)
	}

	// Drain whatever arrived between the last tick and shutdown, then flush the
	// writer. Without this, buffered envelopes are lost on exit.
	if runErr == nil {
		runErr = tailer.Poll()
	}
	if err := sink.Flush(); err != nil && runErr == nil {
		runErr = err
	}
	if runErr != nil {
		fmt.Fprintln(os.Stderr, runErr)
		os.Exit(1)
	}
}
