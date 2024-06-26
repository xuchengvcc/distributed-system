package main

//
// start a worker process, which is implemented
// in ../mr/worker.go. typically there will be
// multiple worker processes, talking to one coordinator.
//
// go run mrworker.go wc.so
//
// Please do not change this file.
//

<<<<<<< HEAD
import "6.5840/mr"
import "plugin"
import "os"
import "fmt"
import "log"
=======
import (
	"fmt"
	"log"
	"os"
	"plugin"

	"6.5840/mr"
)
>>>>>>> xucheng-240506-lab1

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so\n")
		os.Exit(1)
	}
<<<<<<< HEAD

	mapf, reducef := loadPlugin(os.Args[1])

=======
	mapf, reducef := loadPlugin(os.Args[1])
	// numWorkers := 10
	// // fmt.Printf("开启 %v 个协程\n", numWorkers)
	// var wg sync.WaitGroup
	// for i := 0; i < numWorkers; i++ {
	// 	wg.Add(1)
	// 	go func() {
	// 		defer wg.Done()
	// 		mr.Worker(mapf, reducef)
	// 	}()
	// }
	// wg.Wait()
>>>>>>> xucheng-240506-lab1
	mr.Worker(mapf, reducef)
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
