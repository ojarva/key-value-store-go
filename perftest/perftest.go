package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"
)

func fetchStats(host string, port int) {
	c, err := net.Dial("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		fmt.Println("Unable to connect:", err)
		return
	}
	defer c.Close()
	fmt.Fprint(c, "stats\n")
	incoming, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		fmt.Println("Unable to read stats:", err)
		return
	}
	fmt.Println(incoming)
	fmt.Fprint(c, "reset stats\n")
	incoming, err = bufio.NewReader(c).ReadString('\n')
	if err != nil {
		fmt.Println("Unable to reset stats:", err)
		return
	}
}

func testClient(host string, port int, wg *sync.WaitGroup, cmdCount int, channel chan int64) {
	defer wg.Done()
	c, err := net.Dial("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		fmt.Println("Unable to connect:", err)
		return
	}
	defer c.Close()
	startTime := time.Now().UnixNano()
	reader := bufio.NewReader(c)
	for i := 0; i < cmdCount; i++ {
		validKey := rand.Int63()
		fmt.Fprintf(c, "set %d w%d\n", validKey, rand.Int63())
		fmt.Fprintf(c, "set %d n%d\n", validKey, rand.Int63())
		fmt.Fprintf(c, "get %d\n", validKey)
		fmt.Fprintf(c, "get invalidkey%d\n", rand.Int63())
		_, err = reader.ReadString('\n')
		if err != nil {
			fmt.Printf("Failed: %s\n", err)
		}
	}
	endTime := time.Now().UnixNano()
	channel <- (endTime - startTime)
}

func main() {
	hostFlag := flag.String("host", "localhost", "Host to connect to")
	portFlag := flag.Int("port", 1234, "Port to connect")
	threadCountFlag := flag.Int("thread-count", 100, "Number of threads to spawn")
	cmdCountFlag := flag.Int("cmd-count", 100, "Number of commands to send")
	flag.Parse()
	var wg sync.WaitGroup
	startTime := time.Now()
	timeChan := make(chan int64, *threadCountFlag)
	for i := 0; i < *threadCountFlag; i++ {
		wg.Add(1)
		go testClient(*hostFlag, *portFlag, &wg, *cmdCountFlag, timeChan)
	}
	wg.Wait()
	endTime := time.Now()
	close(timeChan)
	var totalCommandTime int64
	for cmdTime := range timeChan {
		totalCommandTime = totalCommandTime + cmdTime
	}

	fmt.Printf("Running %d threads took %dms\n", *threadCountFlag, (endTime.UnixNano()-startTime.UnixNano())/int64(time.Millisecond))
	fmt.Printf("Total time executing commands: %dms\n", totalCommandTime/int64(time.Millisecond))
	fmt.Printf("Total time per command: %fns\n", float64(totalCommandTime)/float64(*cmdCountFlag)/float64(*threadCountFlag)/4)
}
