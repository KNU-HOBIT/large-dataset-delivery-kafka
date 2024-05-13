package util

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// SetupSignalHandling configures handling of SIGINT and SIGTERM for graceful shutdown.
func SetupSignalHandling() {
	sigChan := make(chan os.Signal, 1)
	// Notify sigChan when SIGINT or SIGTERM is received.
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan // Block until a signal is received.
		fmt.Printf("Signal received: %v\n", sig)
		time.Sleep(1 * time.Second) // 1초간 대기
		os.Exit(0)                  // Exit the program after handling the signal.
	}()
}
