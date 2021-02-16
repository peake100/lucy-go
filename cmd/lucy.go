package main

import (
	"github.com/peake100/lucy-go/internal/service"
)

func main() {
	manager, logger := service.NewLucyManager()

	err := manager.Run()
	if err != nil {
		logger.Err(err).Msg("service manager run")
	}
}
