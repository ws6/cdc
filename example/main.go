package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/denisenkom/go-mssqldb" //for msdb driver
	_ "github.com/ws6/calculator/extraction/progressor/msdbprogressor"
	"github.com/ws6/calculator/runner"

	_ "github.com/ws6/cdc"
)

func main() {

	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()

	shutDownChan := make(chan os.Signal)
	signal.Notify(shutDownChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-shutDownChan:
			cancelFn()
			fmt.Println(`app get canceled`)
		}
	}()
	runner.Run(ctx)
}
