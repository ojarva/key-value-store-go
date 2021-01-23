package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ojarva/key-value-store-go/storage"
)

var customLogger *log.Logger

func sendLine(c net.Conn, statusCode int, line string, writeTimeout *time.Duration) {
	c.SetDeadline(time.Now().Add(*writeTimeout))
	c.Write([]byte(fmt.Sprintf("%d %s\n", statusCode, line)))
}

type timeoutSettings struct {
	GeneralTimeout time.Duration
	WriteTimeout   time.Duration
}

type response struct {
	StatusCode   int
	Text         string
	FinalCommand func()
}

func getCommand(c net.Conn, args string, dataContainer *dataContainer) response {
	parts := strings.Split(args, " ")
	if len(parts) != 2 {
		return response{StatusCode: 400, Text: "Invalid command. get expects a single parameter (key)"}
	}
	value, found := dataContainer.KeyValueMap.GetKey(parts[1])
	if !found {
		return response{StatusCode: 404, Text: "Key not found"}
	}
	return response{StatusCode: 200, Text: string(value)}
}

func setCommand(c net.Conn, args string, dataContainer *dataContainer) response {
	parts := strings.Split(args, " ")
	if len(parts) != 3 {
		return response{StatusCode: 400, Text: "Invalid command. Set expects at least two arguments (key and value)"}
	}
	// TODO: handle quoting
	// TODO: don't convert from incoming bytes to strings and back to bytes
	dataContainer.KeyValueMap.SetKey(parts[1], []byte(parts[2]))
	return response{StatusCode: 201, Text: "Created"}
}

func resetCommand(c net.Conn, args string, dataContainer *dataContainer) response {
	args = strings.Trim(args, " ")
	switch args {
	case "stats":
		(*dataContainer).StatsResetChannel <- true
		return response{StatusCode: 200, Text: "Stats reset"}
	default:
		return response{StatusCode: 400, Text: "Invalid command. Stats requires type to be reset"}
	}
}

func pingCommand(c net.Conn, args string, dataContainer *dataContainer) response {
	return response{StatusCode: 200, Text: "pong" + args}
}

func statsCommand(c net.Conn, args string, dataContainer *dataContainer) response {
	statsOutput := "stats"
	connectionStatsChannel := make(chan string, 1)
	(*dataContainer).StatsRequestChannel <- connectionStatsChannel
	statsOutput += <-connectionStatsChannel
	return response{StatusCode: 200, Text: statsOutput}
}

func keyCountCommand(c net.Conn, args string, dataContainer *dataContainer) response {
	keyCount := (*dataContainer).KeyValueMap.GetKeyCount()
	if keyCount == -1 {
		return response{StatusCode: 501, Text: "Not implemented. Current store backend does not support key count"}
	}
	return response{StatusCode: 200, Text: fmt.Sprintf("keycount %d", keyCount)}
}

func quitCommand(c net.Conn, args string, dataContainer *dataContainer) response {
	closeFunc := func() {
		c.Close()
	}
	return response{StatusCode: 200, Text: "Bye", FinalCommand: closeFunc}
}

type commandParams struct {
	Func func(net.Conn, string, *dataContainer) response
	Help string
}

type commandMap map[string]*commandParams

var commands commandMap

type dataContainer struct {
	KeyValueMap         storage.KVMap
	Commands            *commandMap
	StatsRequestChannel chan chan string
	StatsResetChannel   chan bool
	StatsChannel        chan statsPoint
	TimeoutSettings     timeoutSettings
}

func handleIncomingCommand(c net.Conn, dataContainer *dataContainer, commandMap *commandMap, incoming string) response {
	var command string
	var args string
	incoming = strings.TrimRight(incoming, "\n")
	firstSpace := strings.IndexRune(incoming, ' ')
	if firstSpace != -1 {
		command = incoming[0:firstSpace]
		args = incoming[firstSpace:]
	} else {
		command = incoming
		args = ""
	}
	_, found := (*commandMap)[command]
	var r response
	if found {
		r = (*commandMap)[command].Func(c, args, dataContainer)
		dataContainer.StatsChannel <- statsPoint{Status: r.StatusCode, Command: command}
	} else {
		r = response{StatusCode: 400, Text: "Invalid command."}
	}
	return r
}

func handleConnection(c net.Conn, dataContainer *dataContainer) {
	connectionOpenTime := time.Now()
	customLogger.Printf("Serving %s", c.RemoteAddr())
	c.SetDeadline(time.Now().Add(dataContainer.TimeoutSettings.GeneralTimeout))
	defer c.Close()
	defer customLogger.Printf("Stopped serving %s. Connection opened at %s", c.RemoteAddr(), connectionOpenTime)
	commandMap := (*dataContainer).Commands
	reader := bufio.NewReader(c)
	for {
		c.SetDeadline(time.Now().Add(dataContainer.TimeoutSettings.GeneralTimeout))
		incoming, err := reader.ReadString('\n')
		if err != nil {
			customLogger.Printf("Unable to read from %s: %s", c.RemoteAddr(), err)
			break
		}
		r := handleIncomingCommand(c, dataContainer, commandMap, incoming)
		sendLine(c, r.StatusCode, r.Text, &dataContainer.TimeoutSettings.WriteTimeout)
		if r.FinalCommand != nil {
			r.FinalCommand()
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

func statsCollector(incomingChannel <-chan statsPoint, statsRequestChannel <-chan chan string, statsResetChannel <-chan bool, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()
	statsMap := make(map[string]*statsContainer)
	var sp statsPoint
	var strchan chan string
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
			strchan <- statsOutput
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
	statsRequestChannel := make(chan chan string)
	statsResetChannel := make(chan bool)
	// statsChannel is quite busy so we allow some buffering.
	statsChannel := make(chan statsPoint, 100)

	dataContainer := dataContainer{KeyValueMap: kvMap, Commands: &commands, StatsRequestChannel: statsRequestChannel, StatsResetChannel: statsResetChannel, TimeoutSettings: timeoutSettings, StatsChannel: statsChannel}
	return &addr, &dataContainer, nil
}

func run(dataContainer *dataContainer, addr *net.TCPAddr) error {
	var wg sync.WaitGroup
	l, err := net.ListenTCP("tcp4", addr)
	if err != nil {
		return err
	}

	go statsCollector(dataContainer.StatsChannel, dataContainer.StatsRequestChannel, dataContainer.StatsResetChannel, &wg)

	customLogger.Printf("Listening on %s:%d with %s for inactivity timeout and %s for write timeout", addr.IP, addr.Port, dataContainer.TimeoutSettings.GeneralTimeout, dataContainer.TimeoutSettings.WriteTimeout)
	defer l.Close()

	for {
		client, err := l.Accept()
		if err != nil {
			customLogger.Print(err)
			continue
		}
		go handleConnection(client, dataContainer)
	}
}

func main() {
	portFlag := flag.Int("port", 1234, "Port to listen on")
	ipFlag := flag.String("ip", "0.0.0.0", "IP address to listen on")
	mapNameFlag := flag.String("map-name", "basic", "Map format to use")
	generalTimeoutFlag := flag.String("general-timeout", "5s", "General connection inactivity timeout")
	writeTimeoutFlag := flag.String("write-timeout", "1s", "Write timeout")
	flag.Parse()
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
}
