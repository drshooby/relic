package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
)

const logBottlePathWithoutHome = "Library/Application Support/CrossOver/Bottles/Steam Library/drive_c/users/crossover/AppData/Local/Warframe/EE.log"

// shutdownTimeout bounds the final drain and flush after the signal arrives.
const shutdownTimeout = 10 * time.Second

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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Logs go to stderr so stdout stays a clean envelope stream that can be
	// piped or redirected.
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	// The tailer logs through the package default rather than carrying a logger
	// of its own; the sink takes one explicitly so tests can capture it.
	slog.SetDefault(log)

	var sink Sink
	if *sinkFlag == "kinesis" {
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			log.Error("load aws config", "err", err)
			os.Exit(1)
		}
		ks, err := NewKinesisSink(ctx, cfg, log)
		if err != nil {
			log.Error("build kinesis sink", "err", err)
			os.Exit(1)
		}
		sink = ks
	} else {
		sink = NewStdoutSink(os.Stdout)
	}

	tailer, err := NewTailer(path, sink)
	if err != nil {
		log.Error("open log", "err", err)
		os.Exit(1)
	}
	defer tailer.Close()

	var runErr error
	if *once {
		runErr = tailer.Poll(ctx)
	} else {
		log.Info("tailing", "path", path, "sink", *sinkFlag)
		runErr = tailer.Run(ctx)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := tailer.Shutdown(shutdownCtx); err != nil && runErr == nil {
		runErr = err
	}

	if runErr != nil {
		log.Error("exiting", "err", runErr)
		os.Exit(1)
	}
	log.Info("stopped")
}
