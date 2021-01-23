package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/ojarva/key-value-store-go/storage"
)

var customLogger *log.Logger

func sendLine(c net.Conn, statusCode int, line []byte, writeTimeout *time.Duration) {
	c.SetDeadline(time.Now().Add(*writeTimeout))
	c.Write([]byte(fmt.Sprintf("%d ", statusCode)))
	c.Write(line)
	c.Write([]byte("\n"))
}

type timeoutSettings struct {
	GeneralTimeout time.Duration
	WriteTimeout   time.Duration
}

type response struct {
	StatusCode   int
	Text         []byte
	FinalCommand func()
}

func getCommand(c net.Conn, args []byte, dataContainer *dataContainer, connectionContainer *connectionContainer) response {
	if bytes.ContainsRune(args, ' ') || len(args) == 0 {
		return response{StatusCode: 400, Text: []byte("Invalid command. get expects a single parameter (key)")}
	}
	value, found := dataContainer.KeyValueMap.GetKey(string(args))
	if !found {
		return response{StatusCode: 404, Text: []byte("Key not found")}
	}
	return response{StatusCode: 200, Text: value}
}

func deleteCommand(c net.Conn, args []byte, dataContainer *dataContainer, connectionContainer *connectionContainer) response {
	if bytes.ContainsRune(args, ' ') || len(args) == 0 {
		return response{StatusCode: 400, Text: []byte("Invalid command. Delete expects one argument (key)")}
	}
	keyName := string(args)
	dataContainer.KeyValueMap.DeleteKey(keyName)
	dataContainer.ChangesChannel <- Command{Command: "delete", Key: keyName}
	return response{StatusCode: 200, Text: []byte("Deleted")}
}

func setCommand(c net.Conn, args []byte, dataContainer *dataContainer, connectionContainer *connectionContainer) response {
	firstSpace := bytes.IndexRune(args, ' ')
	if firstSpace == -1 {
		return response{StatusCode: 400, Text: []byte("Invalid command. Set expects key and value as parameters")}
	}
	keyName := string(args[0:firstSpace])
	value := args[firstSpace+1:]
	dataContainer.KeyValueMap.SetKey(keyName, value)
	dataContainer.ChangesChannel <- Command{Command: "set", Key: keyName, Value: value}
	return response{StatusCode: 201, Text: []byte("Created")}
}

func resetCommand(c net.Conn, args []byte, dataContainer *dataContainer, connectionContainer *connectionContainer) response {
	if bytes.Compare(args, []byte("stats")) == 0 {
		dataContainer.StatsResetChannel <- true
		return response{StatusCode: 200, Text: []byte("Stats reset")}
	} else {
		return response{StatusCode: 400, Text: []byte("Invalid command. Stats requires type to be reset")}
	}
}

func subscribeCommand(c net.Conn, args []byte, dataContainer *dataContainer, connectionContainer *connectionContainer) response {
	dataContainer.SubscriptionChannel <- SubscriptionCommand{UnsubscribeSender: false, SenderID: connectionContainer.SenderID, SubscriptionChannel: connectionContainer.SubscriptionChannel}
	return response{StatusCode: 200, Text: []byte("Subscribed")}
}

func unsubscribeCommand(c net.Conn, args []byte, dataContainer *dataContainer, connectionContainer *connectionContainer) response {
	dataContainer.SubscriptionChannel <- SubscriptionCommand{UnsubscribeSender: true, SenderID: connectionContainer.SenderID}
	return response{StatusCode: 200, Text: []byte("Unsubscribed")}
}

func pingCommand(c net.Conn, args []byte, dataContainer *dataContainer, connectionContainer *connectionContainer) response {
	ret := []byte("pong ")
	ret = append(ret, args...)
	return response{StatusCode: 200, Text: ret}
}

func statsCommand(c net.Conn, args []byte, dataContainer *dataContainer, connectionContainer *connectionContainer) response {
	statsOutput := []byte("stats")
	connectionStatsChannel := make(chan []byte, 1)
	dataContainer.StatsRequestChannel <- connectionStatsChannel
	statsOutput = append(statsOutput, (<-connectionStatsChannel)...)
	return response{StatusCode: 200, Text: statsOutput}
}

func keyCountCommand(c net.Conn, args []byte, dataContainer *dataContainer, connectionContainer *connectionContainer) response {
	keyCount := dataContainer.KeyValueMap.GetKeyCount()
	if keyCount == -1 {
		return response{StatusCode: 501, Text: []byte("Not implemented. Current store backend does not support key count")}
	}
	return response{StatusCode: 200, Text: []byte(fmt.Sprintf("keycount %d", keyCount))}
}

func quitCommand(c net.Conn, args []byte, dataContainer *dataContainer, connectionContainer *connectionContainer) response {
	if bytes.Compare(args, []byte("server")) == 0 {
		close(dataContainer.QuitChannel)
		return response{StatusCode: 200, Text: []byte("Bye, server exiting")}
	} else if bytes.Compare(args, []byte("client")) == 0 || bytes.Compare(args, []byte("")) == 0 {
		closeFunc := func() {
			c.Close()
		}
		return response{StatusCode: 200, Text: []byte("Bye"), FinalCommand: closeFunc}
	}
	return response{StatusCode: 400, Text: []byte("Invalid quit command")}
}

type commandParams struct {
	Func func(net.Conn, []byte, *dataContainer, *connectionContainer) response
	Help string
}

type Command struct {
	Command string
	Key     string
	Value   []byte
}

func (c *Command) Format() []byte {
	if len(c.Key) > 0 {
		if len(c.Value) > 0 {
			return []byte(fmt.Sprintf("%s %s %s", c.Command, c.Key, c.Value))
		}
		return []byte(fmt.Sprintf("%s %s", c.Command, c.Key))
	}
	return []byte(c.Command)
}

type SubscriptionCommand struct {
	SubscriptionChannel chan Command
	UnsubscribeSender   bool
	SenderID            string
}

type commandMap map[string]*commandParams

var commands commandMap

type dataContainer struct {
	KeyValueMap         storage.KVMap
	Commands            *commandMap
	StatsRequestChannel chan chan []byte
	StatsResetChannel   chan bool
	StatsChannel        chan statsPoint
	SubscriptionChannel chan SubscriptionCommand
	ChangesChannel      chan Command
	SenderIDGenerator   func() string
	TimeoutSettings     timeoutSettings
	QuitChannel         chan struct{}
}

type connectionContainer struct {
	SenderID            string
	SubscriptionChannel chan Command
}

func generateSenderID() func() string {
	var seq int
	var mutex sync.Mutex
	return func() string {
		mutex.Lock()
		defer mutex.Unlock()
		seq++
		return fmt.Sprintf("%d", seq)
	}
}

func subscriptionService(subscriptionChannel chan SubscriptionCommand, changesChannel chan Command) {
	subscriptions := make(map[string]SubscriptionCommand)
	for {
		select {
		case sc := <-subscriptionChannel:
			if sc.UnsubscribeSender == true {
				_, found := subscriptions[sc.SenderID]
				if found {
					delete(subscriptions, sc.SenderID)
				}
			} else {
				_, found := subscriptions[sc.SenderID]
				if found {
					delete(subscriptions, sc.SenderID)
				}
				subscriptions[sc.SenderID] = sc
			}
		case c := <-changesChannel:
			for _, subscriber := range subscriptions {
				subscriber.SubscriptionChannel <- c
			}
		}
	}
}

func handleIncomingCommand(c net.Conn, dataContainer *dataContainer, commandMap *commandMap, incoming []byte, connectionContainer *connectionContainer) response {
	var command string
	var args []byte
	firstSpace := bytes.IndexRune(incoming, ' ')
	if firstSpace != -1 {
		command = string(incoming[0:firstSpace])
		args = incoming[firstSpace+1:]
	} else {
		command = string(incoming)
		args = nil
	}
	_, found := (*commandMap)[command]
	var r response
	if found {
		r = (*commandMap)[command].Func(c, args, dataContainer, connectionContainer)
		dataContainer.StatsChannel <- statsPoint{Status: r.StatusCode, Command: command}
	} else {
		r = response{StatusCode: 400, Text: []byte(fmt.Sprintf("Invalid command: %s", command))}
	}
	return r
}

func handleConnection(c net.Conn, dataContainer *dataContainer) {
	connectionContainer := &connectionContainer{
		SenderID:            dataContainer.SenderIDGenerator(),
		SubscriptionChannel: make(chan Command, 100),
	}
	connectionOpenTime := time.Now()
	customLogger.Printf("Serving %s", c.RemoteAddr())
	c.SetDeadline(time.Now().Add(dataContainer.TimeoutSettings.GeneralTimeout))
	defer c.Close()
	defer customLogger.Printf("Stopped serving %s. Connection opened at %s", c.RemoteAddr(), connectionOpenTime)
	commandMap := (*dataContainer).Commands
	incomingLineChan := make(chan []byte, 10)
	go func() {
		reader := bufio.NewReader(c)
		for {
			incoming, _, err := reader.ReadLine()
			if err != nil {
				customLogger.Printf("Unable to read from %s: %s", c.RemoteAddr(), err)
				close(incomingLineChan)
				return
			}
			if len(incoming) > 0 {
				incomingLineChan <- incoming
			}
		}
	}()
	for {
		c.SetDeadline(time.Now().Add(dataContainer.TimeoutSettings.GeneralTimeout))
		select {
		case incoming := <-incomingLineChan:
			if len(incoming) == 0 {
				return
			}
			r := handleIncomingCommand(c, dataContainer, commandMap, incoming, connectionContainer)
			sendLine(c, r.StatusCode, r.Text, &dataContainer.TimeoutSettings.WriteTimeout)
			if r.FinalCommand != nil {
				r.FinalCommand()
			}
		case sm := <-connectionContainer.SubscriptionChannel:
			sendLine(c, 200, sm.Format(), &dataContainer.TimeoutSettings.WriteTimeout)
		}
	}
}

type statsPoint struct {
	Command string
	Status  int
}
type statsContainer struct {
	S404 uint64
	S200 uint64
	S201 uint64
	S400 uint64
}

func statsCollector(incomingChannel <-chan statsPoint, statsRequestChannel <-chan chan []byte, statsResetChannel <-chan bool, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()
	statsMap := make(map[string]*statsContainer)
	var sp statsPoint
	var strchan chan []byte
	for {
		select {
		case sp = <-incomingChannel:
			if _, found := statsMap[sp.Command]; !found {
				statsMap[sp.Command] = &statsContainer{}
			}
			switch sp.Status {
			case 200:
				statsMap[sp.Command].S200++
			case 201:
				statsMap[sp.Command].S201++
			case 400:
				statsMap[sp.Command].S400++
			case 404:
				statsMap[sp.Command].S404++

			}
		case strchan = <-statsRequestChannel:
			var statsOutput string
			for commandName, stats := range statsMap {
				statsOutput += fmt.Sprintf(" %s_200=%d %s_201=%d %s_404=%d %s_400=%d", commandName, stats.S200, commandName, stats.S201, commandName, stats.S404, commandName, stats.S400)
			}
			strchan <- []byte(statsOutput)
			// This would get garbage collected, but there's no reason not to explicitly close as well.
			close(strchan)
		case _ = <-statsResetChannel:
			statsMap = make(map[string]*statsContainer)
			customLogger.Print("Stats reset")
		}
	}
}

func init() {
	customLogger = log.New(os.Stderr, "", log.Ldate|log.Lmicroseconds|log.LUTC|log.Llongfile)
	commands = make(commandMap)
	commands["set"] = &commandParams{Func: setCommand, Help: "set <key> <value> sets a new key"}
	commands["get"] = &commandParams{Func: getCommand, Help: "get <key> gets a key or returns an error"}
	commands["stats"] = &commandParams{Func: statsCommand, Help: "stats dumps out stats"}
	commands["reset"] = &commandParams{Func: resetCommand, Help: "reset <key> resets information"}
	commands["quit"] = &commandParams{Func: quitCommand, Help: "quit closes the connection"}
	commands["ping"] = &commandParams{Func: pingCommand, Help: "ping <id> returns pong <id>"}
	commands["keycount"] = &commandParams{Func: keyCountCommand, Help: "keycount prints number of keys"}
	commands["subscribe"] = &commandParams{Func: subscribeCommand, Help: "Subscribe to changes"}
	commands["unsubscribe"] = &commandParams{Func: unsubscribeCommand, Help: "Unsubscribe from changes stream"}
}

func getKvMap(mapName string) (storage.KVMap, error) {
	switch mapName {
	case "basic":
		return &storage.BasicKvMap{}, nil
	case "sharded":
		return &storage.ShardedKvMap{}, nil
	case "sync":
		return &storage.SyncKvMap{}, nil
	case "race":
		return &storage.RaceKvMap{}, nil
	case "filebacked":
		return &storage.FileBackedStorage{}, nil
	case "cachefilebacked":
		return &storage.CachedFileBackedStorage{}, nil
	default:
		return nil, errors.New("Invalid map name")
	}
}

func initialize(port int, ip string, mapName string, generalTimeout string, writeTimeout string) (*net.TCPAddr, *dataContainer, error) {
	parsedIP := net.ParseIP(ip)
	if parsedIP == nil {
		return nil, nil, errors.New("Invalid IP address; unable to parse")
	}
	generalTimeoutDuration, err := time.ParseDuration(generalTimeout)
	if err != nil {
		return nil, nil, err
	}
	writeTimeoutDuration, err := time.ParseDuration(writeTimeout)
	if err != nil {
		return nil, nil, err
	}
	var kvMap storage.KVMap
	kvMap, err = getKvMap(mapName)
	if err != nil {
		return nil, nil, err
	}
	kvMap.Init()
	timeoutSettings := timeoutSettings{GeneralTimeout: generalTimeoutDuration, WriteTimeout: writeTimeoutDuration}
	addr := net.TCPAddr{IP: parsedIP, Port: port}

	// Requesting goroutines should block for these; there's no reason for command handlers to proceed before these are done.
	statsRequestChannel := make(chan chan []byte)
	statsResetChannel := make(chan bool)
	// statsChannel is quite busy so we allow some buffering.
	statsChannel := make(chan statsPoint, 100)

	dataContainer := dataContainer{
		KeyValueMap:         kvMap,
		Commands:            &commands,
		StatsRequestChannel: statsRequestChannel,
		StatsResetChannel:   statsResetChannel,
		TimeoutSettings:     timeoutSettings,
		StatsChannel:        statsChannel,
		SubscriptionChannel: make(chan SubscriptionCommand, 100),
		ChangesChannel:      make(chan Command, 100),
		SenderIDGenerator:   generateSenderID(),
		QuitChannel:         make(chan struct{}, 1),
	}
	return &addr, &dataContainer, nil
}

func run(dataContainer *dataContainer, addr *net.TCPAddr) error {
	var wg sync.WaitGroup
	l, err := net.ListenTCP("tcp4", addr)
	if err != nil {
		return err
	}

	go statsCollector(dataContainer.StatsChannel, dataContainer.StatsRequestChannel, dataContainer.StatsResetChannel, &wg)
	go subscriptionService(dataContainer.SubscriptionChannel, dataContainer.ChangesChannel)
	customLogger.Printf("Listening on %s:%d with %s for inactivity timeout and %s for write timeout", addr.IP, addr.Port, dataContainer.TimeoutSettings.GeneralTimeout, dataContainer.TimeoutSettings.WriteTimeout)
	defer l.Close()
	newConnectionChannel := make(chan net.Conn, 100)

	go func() {
		for {
			client, err := l.Accept()
			if err != nil {
				customLogger.Print(err)
				close(newConnectionChannel)
				return
			}
			newConnectionChannel <- client
		}
	}()

	for {
		select {
		case client := <-newConnectionChannel:
			go handleConnection(client, dataContainer)
		case _ = <-dataContainer.QuitChannel:
			customLogger.Print("Quit command received, quitting")
			return nil
		}
	}
}

func main() {
	portFlag := flag.Int("port", 1234, "Port to listen on")
	ipFlag := flag.String("ip", "0.0.0.0", "IP address to listen on")
	mapNameFlag := flag.String("map-name", "basic", "Map format to use")
	generalTimeoutFlag := flag.String("general-timeout", "5s", "General connection inactivity timeout")
	writeTimeoutFlag := flag.String("write-timeout", "1s", "Write timeout")
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	memprofile := flag.String("memprofile", "", "write memory profile to file")
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	addr, dataContainer, err := initialize(*portFlag, *ipFlag, *mapNameFlag, *generalTimeoutFlag, *writeTimeoutFlag)
	if err != nil {
		customLogger.Fatal(err)
		os.Exit(1)
	}
	err = run(dataContainer, addr)
	if err != nil {
		customLogger.Fatal(err)
		os.Exit(1)
	}
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		runtime.GC()    // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
}
