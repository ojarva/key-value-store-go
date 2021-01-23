package main

import (
	"bufio"
	"bytes"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/ojarva/key-value-store-go/storage"
)

func getDummyDataContainer() dataContainer {
	kvMap := &storage.BasicKvMap{}
	kvMap.Init()
	statsRequestChannel := make(chan chan string, 10)
	statsResetChannel := make(chan bool, 10)
	statsChannel := make(chan statsPoint, 100)
	timeout, _ := time.ParseDuration("5s")
	return dataContainer{
		KeyValueMap:         kvMap,
		Commands:            &commands,
		StatsRequestChannel: statsRequestChannel,
		StatsResetChannel:   statsResetChannel,
		StatsChannel:        statsChannel,
		ChangesChannel:      make(chan Command, 100),
		SubscriptionChannel: make(chan SubscriptionCommand, 100),
		SenderIDGenerator:   generateSenderID(),
		TimeoutSettings: timeoutSettings{
			GeneralTimeout: timeout,
			WriteTimeout:   timeout,
		},
	}
}

func TestStatsCollector(t *testing.T) {
	incomingChannel := make(chan statsPoint)
	statsRequestChannel := make(chan chan string)
	statsResetChannel := make(chan bool)
	var wg sync.WaitGroup
	go statsCollector(incomingChannel, statsRequestChannel, statsResetChannel, &wg)
	statsRequest := make(chan string)
	statsRequestChannel <- statsRequest
	var statsResponse string
	statsResponse = <-statsRequest
	if statsResponse != "" {
		t.Errorf("Empty statsCollector responded with stats: %s", statsResponse)
	}
	incomingChannel <- statsPoint{"get", 200}
	statsRequest = make(chan string)
	statsRequestChannel <- statsRequest
	statsResponse = <-statsRequest
	if statsResponse != " get_200=1 get_201=0 get_404=0 get_400=0" {
		t.Errorf("Invalid response from statsCollector for a single metric check: %s", statsResponse)
	}
	incomingChannel <- statsPoint{"get", 201}
	incomingChannel <- statsPoint{"get", 400}
	incomingChannel <- statsPoint{"get", 404}
	incomingChannel <- statsPoint{"get", 404}
	statsRequest = make(chan string)
	statsRequestChannel <- statsRequest
	statsResponse = <-statsRequest

	if statsResponse != " get_200=1 get_201=1 get_404=2 get_400=1" {
		t.Errorf("Invalid response from statsCollector for multiple check: %s", statsResponse)
	}
	statsResetChannel <- true
	statsRequest = make(chan string)
	statsRequestChannel <- statsRequest
	statsResponse = <-statsRequest
	if statsResponse != "" {
		t.Errorf("Empty statsCollector responded with stats: %s", statsResponse)
	}
}

func TestHandleIncomingCommand(t *testing.T) {
	client, _ := net.Pipe()

	dataContainer := getDummyDataContainer()
	connectionContainer := &connectionContainer{}
	commandMap := dataContainer.Commands

	var r response
	r = handleIncomingCommand(client, &dataContainer, commandMap, "invalid command", connectionContainer)
	if r.StatusCode != 400 {
		t.Errorf("Invalid command produced incorrect response: %d", r.StatusCode)
	}
	r = handleIncomingCommand(client, &dataContainer, commandMap, "invalid-command", connectionContainer)
	if r.StatusCode != 400 {
		t.Errorf("Invalid command produced incorrect response: %d", r.StatusCode)
	}

	r = handleIncomingCommand(client, &dataContainer, commandMap, "quit", connectionContainer)
	if r.StatusCode != 200 {
		t.Errorf("Requesting stats produced incorrect response: %d", r.StatusCode)
	}
	sp := <-dataContainer.StatsChannel
	if sp.Status != r.StatusCode {
		t.Errorf("Stats point status code does not match to response status code: %d <> %d", sp.Status, r.StatusCode)
	}
}

func TestKeyCountCommand(t *testing.T) {
	dataContainer := getDummyDataContainer()
	connectionContainer := &connectionContainer{}
	client, _ := net.Pipe()
	var r response
	r = keyCountCommand(client, "", &dataContainer, connectionContainer)
	if r.StatusCode != 200 {
		t.Errorf("keycount returned incorrect status code %d", r.StatusCode)
	}
	dataContainer.KeyValueMap = &storage.SyncKvMap{}
	dataContainer.KeyValueMap.Init()
	r = keyCountCommand(client, "", &dataContainer, connectionContainer)
	if r.StatusCode != 501 {
		t.Errorf("keycount returned incorrect status code %d", r.StatusCode)
	}
}

func TestPingCommand(t *testing.T) {
	dataContainer := getDummyDataContainer()
	connectionContainer := &connectionContainer{}
	client, _ := net.Pipe()
	var r response
	r = pingCommand(client, " asdf", &dataContainer, connectionContainer)
	if r.StatusCode != 200 {
		t.Errorf("ping returned incorrect status code %d", r.StatusCode)
	}
	if r.Text != "pong asdf" {
		t.Errorf("ping returned incorrect response %s", r.Text)
	}
}

func TestGetCommand(t *testing.T) {
	dataContainer := getDummyDataContainer()
	connectionContainer := &connectionContainer{}
	client, _ := net.Pipe()
	var r response
	r = getCommand(client, " asdf", &dataContainer, connectionContainer)
	if r.StatusCode != 404 {
		t.Errorf("get returned incorrect status code for invalid key: %d", r.StatusCode)
	}
	r = getCommand(client, "", &dataContainer, connectionContainer)
	if r.StatusCode != 400 {
		t.Errorf("get returned incorrect status code for invalid key: %d", r.StatusCode)
	}
	r = setCommand(client, " asdf foo", &dataContainer, connectionContainer)
	r = getCommand(client, " asdf", &dataContainer, connectionContainer)
	if r.StatusCode != 200 {
		t.Errorf("get returned incorrect status code for valid key: %d", r.StatusCode)
	}
	if r.Text != "foo" {
		t.Errorf("get returned incorrect data for valid key: %s", r.Text)
	}
}

func TestStatsCommand(t *testing.T) {
	dataContainer := getDummyDataContainer()
	connectionContainer := &connectionContainer{}
	client, _ := net.Pipe()
	go func() {
		incomingChannel := <-dataContainer.StatsRequestChannel
		incomingChannel <- " your stats"
	}()
	var r response
	r = statsCommand(client, "", &dataContainer, connectionContainer)
	if r.StatusCode != 200 {
		t.Errorf("stats returned incorrect status code %d", r.StatusCode)
	}
	if r.Text != "stats your stats" {
		t.Errorf("stats did not return our dummy data: %s", r.Text)
	}
}

func TestSubscribeCommand(t *testing.T) {
	dataContainer := getDummyDataContainer()
	connectionContainer := &connectionContainer{SenderID: "mysenderid"}
	client, _ := net.Pipe()
	subscribeCommand(client, "", &dataContainer, connectionContainer)
	sc := <-dataContainer.SubscriptionChannel
	if sc.SenderID != "mysenderid" {
		t.Errorf("subscribeCommand send out invalid senderID: %s", sc.SenderID)
	}
	if sc.UnsubscribeSender {
		t.Errorf("subscribeCommand send out invalid UnsubscribeSender: %t", sc.UnsubscribeSender)
	}
}

func TestUnsubscribeCommand(t *testing.T) {
	dataContainer := getDummyDataContainer()
	connectionContainer := &connectionContainer{SenderID: "mysenderid"}
	client, _ := net.Pipe()
	unsubscribeCommand(client, "", &dataContainer, connectionContainer)
	sc := <-dataContainer.SubscriptionChannel
	if sc.SenderID != "mysenderid" {
		t.Errorf("subscribeCommand send out invalid senderID: %s", sc.SenderID)
	}
	if !sc.UnsubscribeSender {
		t.Errorf("unsubscribeCommand send out invalid UnsubscribeSender: %t", sc.UnsubscribeSender)
	}
}

func TestResetCommand(t *testing.T) {
	dataContainer := getDummyDataContainer()
	connectionContainer := &connectionContainer{}
	client, _ := net.Pipe()
	go func() {
		<-dataContainer.StatsResetChannel
	}()
	var r response
	r = resetCommand(client, "", &dataContainer, connectionContainer)
	if r.StatusCode != 400 {
		t.Errorf("Invalid reset returned incorrect response. %d: %s", r.StatusCode, r.Text)
	}
	r = resetCommand(client, " stats", &dataContainer, connectionContainer)
	if r.StatusCode != 200 {
		t.Errorf("Invalid response from reset: %d: %s", r.StatusCode, r.Text)
	}
}

func TestDeleteCommand(t *testing.T) {
	dataContainer := getDummyDataContainer()
	connectionContainer := &connectionContainer{}
	client, _ := net.Pipe()
	var r response
	r = deleteCommand(client, " asdf", &dataContainer, connectionContainer)
	if r.StatusCode != 200 {
		t.Errorf("delete returned incorrect status code: %d", r.StatusCode)
	}
	select {
	case a := <-dataContainer.ChangesChannel:
		if a.Command != "delete" {
			t.Error("Delete command did not publish delete change")
		}
		if a.Key != "asdf" {
			t.Error("delete command published incorrect key")
		}
	default:
		t.Error("delete command did not publish a change")
	}
	r = deleteCommand(client, "", &dataContainer, connectionContainer)
	if r.StatusCode != 400 {
		t.Errorf("delete returned incorrect status code: %d", r.StatusCode)
	}

}

func TestSetCommand(t *testing.T) {
	dataContainer := getDummyDataContainer()
	connectionContainer := &connectionContainer{}
	client, _ := net.Pipe()
	var r response
	r = setCommand(client, " asdf foo", &dataContainer, connectionContainer)
	if r.StatusCode != 201 {
		t.Errorf("set returned incorrect status code for invalid key: %d", r.StatusCode)
	}
	select {
	case a := <-dataContainer.ChangesChannel:
		if a.Command != "set" {
			t.Error("set command did not publish set change")
		}
		if a.Key != "asdf" {
			t.Error("set command published incorrect key")
		}
		if bytes.Compare(a.Value, []byte("foo")) != 0 {
			t.Error("set command published incorrect value")
		}
	default:
		t.Error("set command did not publish a change")
	}
	r = setCommand(client, " asdf", &dataContainer, connectionContainer)
	if r.StatusCode != 400 {
		t.Errorf("set returned incorrect status code for invalid key: %d", r.StatusCode)
	}
}

func TestInvalidIP(t *testing.T) {
	_, _, ret := initialize(1, "asdf", "basic", "5s", "1s")
	if ret == nil {
		t.Error("Invalid IP did not cause run to fail")
	}
}

func TestInvalidTimeouts(t *testing.T) {
	var ret error
	_, _, ret = initialize(1, "0.0.0.0", "basic", "foo", "1s")
	if ret == nil {
		t.Error("Invalid timeout did not cause run to fail")
	}
	_, _, ret = initialize(1, "0.0.0.0", "basic", "5s", "foo")
	if ret == nil {
		t.Error("Invalid timeout did not cause run to fail")
	}
}

func TestInvalidMapName(t *testing.T) {
	var ret error
	_, _, ret = initialize(1, "0.0.0.0", "invalid", "5s", "1s")
	if ret == nil {
		t.Error("Invalid map name did not cause run to fail")
	}
}

func TestInitialize(t *testing.T) {
	addr, dataContainer, err := initialize(1, "0.0.0.0", "basic", "5s", "1m")
	if addr == nil {
		t.Error("initialize returned nil address")
	}
	if dataContainer == nil {
		t.Error("initialize returned nil dataContainer")
	}
	if err != nil {
		t.Error("initialize returned error")
	}
}

func TestHandleConnection(t *testing.T) {
	client, server := net.Pipe()
	reader := bufio.NewReader(client)
	dataContainer := getDummyDataContainer()
	go handleConnection(server, &dataContainer)
	client.Write([]byte("get foobar\n"))
	incoming, err := reader.ReadString('\n')
	if err != nil {
		t.Errorf("Reading from handleConnection failed with %s", err)
	}
	if incoming != "404 Key not found\n" {
		t.Errorf("GET returned invalid response %s", incoming)
	}
}

func TestHandleQuit(t *testing.T) {
	client, server := net.Pipe()
	reader := bufio.NewReader(client)
	dataContainer := getDummyDataContainer()
	go handleConnection(server, &dataContainer)
	client.Write([]byte("quit\n"))
	incoming, err := reader.ReadString('\n')
	if err != nil {
		t.Errorf("Reading from handleConnection failed with %s", err)
	}
	if incoming != "200 Bye\n" {
		t.Errorf("quit returned invalid response %s", incoming)
	}
}

func TestSubscriptionService(t *testing.T) {
	subscriptionChannel := make(chan SubscriptionCommand, 1)
	changesChannel := make(chan Command, 1)
	go subscriptionService(subscriptionChannel, changesChannel)
	connectionChannel := make(chan Command, 1)
	changesChannel <- Command{Command: "testCommand"}
	time.Sleep(100 * time.Millisecond)
	subscriptionChannel <- SubscriptionCommand{
		UnsubscribeSender:   false,
		SenderID:            "myid",
		SubscriptionChannel: connectionChannel}
	time.Sleep(100 * time.Millisecond)
	changesChannel <- Command{Command: "testCommand2"}
	time.Sleep(100 * time.Millisecond)
	select {
	case cmd := <-connectionChannel:
		if cmd.Command != "testCommand2" {
			t.Errorf("Invalid subscription message from subscription service: %s", cmd)
		}
	default:
		t.Error("No messages from subscription service to subscribed client")
	}
	subscriptionChannel <- SubscriptionCommand{
		UnsubscribeSender:   false,
		SenderID:            "myid",
		SubscriptionChannel: connectionChannel}
	time.Sleep(100 * time.Millisecond)
	subscriptionChannel <- SubscriptionCommand{
		UnsubscribeSender:   true,
		SenderID:            "myid",
		SubscriptionChannel: connectionChannel}
	time.Sleep(100 * time.Millisecond)
	changesChannel <- Command{Command: "testCommand3"}
	time.Sleep(100 * time.Millisecond)
	select {
	case cmd := <-connectionChannel:
		t.Errorf("Message to unsubscribed client: %s", cmd)
	default:
	}
}

func TestCommandFormat(t *testing.T) {
	var command Command
	var formattedCommand string
	command = Command{Command: "mycommand"}
	formattedCommand = command.Format()
	if formattedCommand != "mycommand" {
		t.Errorf("Invalid format for command-only: %s", formattedCommand)
	}
	command.Key = "mykey"
	formattedCommand = command.Format()
	if formattedCommand != "mycommand mykey" {
		t.Errorf("Invalid format for command+key: %s", formattedCommand)
	}
	command.Value = []byte("myvalue")
	formattedCommand = command.Format()
	if formattedCommand != "mycommand mykey myvalue" {
		t.Errorf("Invalid format for command+key+value: %s", formattedCommand)
	}
}
