/*
	Author: ywn202@nyu.edu

	The test cases of the fault tolerant kv-store implemented in Raft.
	Below test cases are testing from the persepctives of clients.

	***** For the testing to run, it assumes we already have started the required kubernetes service pods. *****
	*****                          launch-tool/launch.py boot NUM_RAFT_SERVERS                             *****

	In case client is making the request to non-leader server, it will get a redirect result with leader server name.
	Client can lookup the leader's server ip and port with a call to the python script dealt with kubernetes.

*/

package main

import (
	"testing"

	"bufio"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"time"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/raft/pb"
)

const (
	//launch-tool/launch.py boot 11
	NUM_RAFT_SERVERS = 11
)

type LockedWriter struct {
	m      sync.Mutex
	writer *bufio.Writer
}

func (lw *LockedWriter) Write(str string) (n int, err error) {
	lw.m.Lock()
	defer lw.m.Unlock()
	return lw.writer.WriteString(str)
}

func getKVConnectionToRaftLeader(t *testing.T) (string, pb.KvStoreClient) {
	leaderId := getCurrentLeaderIDByGetRequest(t)
	endpoint := getKVServiceURL(t, leaderId)
	kvc := establishConnection(t, endpoint)
	return leaderId, kvc
}

func establishConnection(t *testing.T, endpoint string) pb.KvStoreClient {
	t.Logf("Connecting to %v", endpoint)
	// Connect to the server. We use WithInsecure since we do not configure https in this class.
	conn, err := grpc.Dial(endpoint, grpc.WithInsecure())
	//Ensure connection did not fail.
	if err != nil {
		t.Fatalf("Failed to dial GRPC server %v", err)
	}
	t.Logf("Connected")

	// Create a KvStore client
	kvc := pb.NewKvStoreClient(conn)

	return kvc
}

func getKVServiceURL(t *testing.T, peerNum string) string {
	cmd := exec.Command("../launch-tool/launch.py", "client-url", peerNum)
	stdout, err := cmd.Output()
	if err != nil {
		t.Fatalf("Cannot get the service URL.")
	}
	endpoint := strings.Trim(string(stdout), "\n")
	return endpoint
}

func failGivenRaftServer(t *testing.T, peerNum string) {
	cmd := exec.Command("../launch-tool/launch.py", "kill", peerNum)
	stdout, err := cmd.Output()
	if err != nil {
		t.Fatalf("Cannot kill given peer server. err: %v", err)
	}
	t.Logf("Killed raft peer%v.", peerNum)
	t.Logf(string(stdout))
}

func relaunchGivenRaftServer(t *testing.T, peerNum string) {
	cmd := exec.Command("../launch-tool/launch.py", "launch", peerNum)
	stdout, err := cmd.Output()
	if err != nil {
		t.Fatalf("Cannot re-launch given peer server.")
	}
	t.Logf(string(stdout))
}

func listAvailRaftServer(t *testing.T) []string {
	cmd := exec.Command("../launch-tool/launch.py", "list")
	stdout, err := cmd.Output()
	if err != nil {
		t.Fatalf("Cannot list Raft servers.")
	}
	re := regexp.MustCompile("[0-9]+")
	return re.FindAllString(string(stdout), -1)
}

func getCurrentLeaderIDByGetRequest(t *testing.T) string {
	redirected := true
	endpoint := getKVServiceURL(t, listAvailRaftServer(t)[0])
	var leaderId string = listAvailRaftServer(t)[0]

	for redirected {
		kvc := establishConnection(t, endpoint)

		// Request value for hello
		req := &pb.Key{Key: "hello"}
		res, err := kvc.Get(context.Background(), req)
		if err != nil {
			t.Fatalf("Request error %v", err)
		}

		switch res.Result.(type) {
		case *pb.Result_Redirect:
			redirected = true
			t.Logf("The given server is not Raft leader, redirect to leader \"%v\" ...", res.GetRedirect().Server)
		default:
			redirected = false
			t.Logf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
		}

		if redirected && res.GetRedirect().Server == "" {
			time.Sleep(2 * time.Second)
		}

		if redirected && res.GetRedirect().Server != "" {
			serverName := strings.Split(res.GetRedirect().Server, ":")[0]
			re := regexp.MustCompile("[0-9]+")
			leaderId = re.FindAllString(serverName, -1)[0]
			endpoint = getKVServiceURL(t, leaderId)
		}
	}

	return leaderId
}

func fireGetRequest(t *testing.T, kvc pb.KvStoreClient, key string, val string, w *LockedWriter, toVerify bool) {
	req := &pb.Key{Key: key}
	if w != nil {
		w.Write(getRequestObjFormatter(t, key))
	}

	res, err := kvc.Get(context.Background(), req)
	if err != nil {
		t.Logf("Request error %v", err)
	}

	if toVerify && (res.GetKv().Key != key || res.GetKv().Value != val) {
		t.Fatalf("We fail to get back what we expect.")
	}

	if w != nil {
		w.Write(getResponseObjFormatter(t, res.GetKv().Key, res.GetKv().Value))
	}
	t.Logf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
}

func fireSetRequest(t *testing.T, kvc pb.KvStoreClient, key string, val string, w *LockedWriter, toVerify bool) {
	//set a key
	putReq := &pb.KeyValue{Key: key, Value: val}
	if w != nil {
		w.Write(setRequestObjFormatter(t, key, val))
	}

	res, err := kvc.Set(context.Background(), putReq)
	if err != nil {
		t.Fatalf("Error while setting a key. err: %v", err)
	}

	if toVerify && (res.GetKv().Key != key || res.GetKv().Value != val) {
		t.Fatalf("Set key returned the wrong response")
	}

	if w != nil {
		w.Write(setResponseObjFormatter(t, res.GetKv().Key, res.GetKv().Value))
	}
	t.Logf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
}

func fireCasRequest(t *testing.T, kvc pb.KvStoreClient, key string, val string, expVal string,
	oldVal string, w *LockedWriter, toVerify bool) {
	casReq := &pb.CASArg{Kv: &pb.KeyValue{Key: key, Value: oldVal}, Value: &pb.Value{Value: val}}
	if w != nil {
		w.Write(casRequestObjFormatter(t, key, val, oldVal))
	}

	res, err := kvc.CAS(context.Background(), casReq)
	if err != nil {
		t.Fatalf("Request error %v", err)
	}

	//this cas success conjecture is not true if the old value is just the value we want to change to;
	//but we expect the old value to be another value.
	//but we can always avoid this case by test cases design.
	success := res.GetKv().Value == val
	if w != nil {
		w.Write(casResponseObjFormatter(t, success, res.GetKv().Key, res.GetKv().Value))
	}
	t.Logf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if toVerify && (res.GetKv().Key != key || res.GetKv().Value != expVal) {
		t.Fatalf("Get returned the wrong response")
	}
}

func fireClearRequest(t *testing.T, kvc pb.KvStoreClient) {
	_, err := kvc.Clear(context.Background(), &pb.Empty{})
	if err != nil {
		t.Fatalf("Could not clear")
	}
}

func fireChangeConfigurationRequest(t *testing.T, kvc pb.KvStoreClient, currList string, newList string) {
	_, err := kvc.ChangeConfiguration(context.Background(), &pb.Servers{CurrList: currList, NewList: newList})
	if err != nil {
		t.Fatalf("Could not change configuration")
	}
}

func goId(t *testing.T) int {
	defer func() {
		if err := recover(); err != nil {
			t.Logf("panic recover:panic info:%v", err)
		}
	}()

	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		t.Fatalf("cannot get goroutine id: %v", err)
	}
	return id
}

//format the logs for linearizability test
func getRequestObjFormatter(t *testing.T, key string) string {
	return fmt.Sprintf("{:process %d, :type :invoke, :f :get, :key \"%v\", :value nil}\n", goId(t), key)
}
func getResponseObjFormatter(t *testing.T, key string, val string) string {
	return fmt.Sprintf("{:process %d, :type :ok, :f :get, :key \"%v\", :value \"%v\"}\n", goId(t), key, val)

}
func setRequestObjFormatter(t *testing.T, key string, val string) string {
	return fmt.Sprintf("{:process %d, :type :invoke, :f :set, :key \"%v\", :value \"%v\"}\n", goId(t), key, val)
}
func setResponseObjFormatter(t *testing.T, key string, val string) string {
	return fmt.Sprintf("{:process %d, :type :ok, :f :set, :key \"%v\", :value \"%v\"}\n", goId(t), key, val)
}
func casRequestObjFormatter(t *testing.T, key string, val string, oldVal string) string {
	return fmt.Sprintf("{:process %d, :type :invoke, :f :cas, :key \"%v\", :value \"%v\", :oldValue \"%v\"}\n", goId(t), key, val, oldVal)
}
func casResponseObjFormatter(t *testing.T, success bool, key string, val string) string {
	return fmt.Sprintf("{:process %d, :type :ok, :f :cas, :success \"%t\", :key \"%v\", :value \"%v\"}\n", goId(t), success, key, val)
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

/*
	Test if clear requests can really clear all existing records
*/
func TestClear(t *testing.T) {
	_, kvc := getKVConnectionToRaftLeader(t)

	fireSetRequest(t, kvc, "x", "99", nil, true)
	fireSetRequest(t, kvc, "y", "88", nil, true)
	fireSetRequest(t, kvc, "z", "18", nil, true)

	fireGetRequest(t, kvc, "x", "99", nil, true)
	fireGetRequest(t, kvc, "y", "88", nil, true)
	fireGetRequest(t, kvc, "z", "18", nil, true)

	fireClearRequest(t, kvc)

	fireGetRequest(t, kvc, "x", "", nil, true)
	fireGetRequest(t, kvc, "y", "", nil, true)
	fireGetRequest(t, kvc, "z", "", nil, true)
}

/*
	Test if we can change configuration successfully without loss of data
*/
func TestChangeConfiguration(t *testing.T) {
	_, kvc := getKVConnectionToRaftLeader(t)
	//fireChangeConfigurationRequest(t, kvc, "peer1:3001,peer2:3001,peer3:3001,peer4:3001,peer0:3001", "peer0:3001,peer2:3001,peer1:3001")
	//fireChangeConfigurationRequest(t, kvc, "peer0:3001,peer2:3001,peer1:3001", "peer1:3001,peer2:3001,peer3:3001,peer4:3001,peer0:3001")
	fireChangeConfigurationRequest(t, kvc, "peer1:3001,peer2:3001,peer3:3001,peer4:3001,peer0:3001", "peer1:3001,peer2:3001,peer3:3001,peer4:3001,peer0:3001,peer5:3001")

}

/*
	Test if cas requests are behaving correctly
*/
func TestCas(t *testing.T) {
	_, kvc := getKVConnectionToRaftLeader(t)

	fireSetRequest(t, kvc, "x", "99", nil, true)
	fireSetRequest(t, kvc, "y", "88", nil, true)
	fireSetRequest(t, kvc, "z", "18", nil, true)

	fireCasRequest(t, kvc, "x", "199", "199", "99", nil, true)
	fireCasRequest(t, kvc, "y", "88", "88", "0000", nil, true)
	fireCasRequest(t, kvc, "k", "199", "199", "", nil, true) // "" empty string is equivalent to unitialized value
	fireCasRequest(t, kvc, "m", "199", "", "??", nil, true)  // unitialised key will not match the old value, fail cas
	fireCasRequest(t, kvc, "z", "118", "18", "000", nil, true)
}

/*
	Test if a Raft server can redirect us to the leader if it is not the leader.
*/
func TestRedirectionHandling(t *testing.T) {
	leaderId := getCurrentLeaderIDByGetRequest(t)

	//fail the leader
	failGivenRaftServer(t, leaderId)
	time.Sleep(20 * time.Second)

	getCurrentLeaderIDByGetRequest(t)

	relaunchGivenRaftServer(t, leaderId)
	//give time for relaunch and let it be stable
	time.Sleep(20 * time.Second)
}

/*
	Test if our changes can survive leader failure.
	After a new leader is selected, the kv-store should return what we previously set.
	1. Set a key for current leader
	2. Fail the current leader
	3. Make a get to new leader, should get back what we set
*/
func TestSurviveLeaderFailure(t *testing.T) {
	leaderId, kvc := getKVConnectionToRaftLeader(t)
	fireSetRequest(t, kvc, "test_leader_failure", "2", nil, true)
	fireSetRequest(t, kvc, "test_leader_failure2", "21", nil, true)
	fireCasRequest(t, kvc, "test_leader_failure2", "999", "21", "99", nil, true)

	//fail the leader
	failGivenRaftServer(t, leaderId)
	//give time for electing new leader
	time.Sleep(20 * time.Second)

	_, kvcNextLeader := getKVConnectionToRaftLeader(t)
	fireGetRequest(t, kvcNextLeader, "test_leader_failure", "2", nil, true)
	fireGetRequest(t, kvcNextLeader, "test_leader_failure2", "21", nil, true)

	//re-launch the previously killed server for other tests
	relaunchGivenRaftServer(t, leaderId)
	//give time for relaunch and let it be stable
	time.Sleep(20 * time.Second)
}

/*
	Test if our changes can survive leader failure after log compaction.
	After a new leader is selected, the kv-store after log compaction should return what we previously set.
	1. Set keys for current leader and triggered log compaction
	2. Fail the current leader
	3. Make a get to new leader, should get back what we set
*/
func TestSurviveLogCompactionAndLeaderFailure(t *testing.T) {
	leaderId, kvc := getKVConnectionToRaftLeader(t)
	t.Run("serial request to trigger log compaction", TestSerialRequestsCorrectness)
	t.Run("serial request to trigger log compaction", TestSerialRequestsCorrectness)
	t.Run("serial request to trigger log compaction", TestSerialRequestsCorrectness)
	fireSetRequest(t, kvc, "test_leader_failure_log_compaction", "2", nil, true)
	fireSetRequest(t, kvc, "test_leader_failure2_log_compaction", "21", nil, true)
	fireCasRequest(t, kvc, "test_leader_failure2_log_compaction", "999", "21", "99", nil, true)

	//fail the leader
	failGivenRaftServer(t, leaderId)
	//give time for electing new leader
	time.Sleep(20 * time.Second)

	_, kvcNextLeader := getKVConnectionToRaftLeader(t)
	fireGetRequest(t, kvcNextLeader, "test_leader_failure_log_compaction", "2", nil, true)
	fireGetRequest(t, kvcNextLeader, "test_leader_failure2_log_compaction", "21", nil, true)

	//re-launch the previously killed server for other tests
	relaunchGivenRaftServer(t, leaderId)
	//give time for relaunch and let it be stable
	time.Sleep(20 * time.Second)
}

/*
	Test it can tolerate f nodes failure given 2f+1 nodes.
	1. Set a key
	2. Fail f nodes
	3. Make a get, should get back what we set
*/
func TestTolerateFFailures(t *testing.T) {
	leaderId, kvc := getKVConnectionToRaftLeader(t)
	fireSetRequest(t, kvc, "test_f_nodes_failure", "3", nil, true)
	fireSetRequest(t, kvc, "test_f_nodes_failure2", "31", nil, true)

	//fail f Raft servers
	//here we starts 5 servers, f = 2
	var nextServerToTry int
	nextServerToTry, _ = strconv.Atoi(leaderId)
	failGivenRaftServer(t, strconv.Itoa((nextServerToTry+1)%NUM_RAFT_SERVERS))
	failGivenRaftServer(t, strconv.Itoa((nextServerToTry+2)%NUM_RAFT_SERVERS))
	failGivenRaftServer(t, strconv.Itoa((nextServerToTry+3)%NUM_RAFT_SERVERS))
	failGivenRaftServer(t, strconv.Itoa((nextServerToTry+4)%NUM_RAFT_SERVERS))
	failGivenRaftServer(t, strconv.Itoa((nextServerToTry+5)%NUM_RAFT_SERVERS))
	time.Sleep(20 * time.Second)

	fireGetRequest(t, kvc, "test_f_nodes_failure", "3", nil, true)
	fireGetRequest(t, kvc, "test_f_nodes_failure2", "31", nil, true)

	//re-launch the previously killed server for other tests
	relaunchGivenRaftServer(t, strconv.Itoa((nextServerToTry+1)%NUM_RAFT_SERVERS))
	relaunchGivenRaftServer(t, strconv.Itoa((nextServerToTry+2)%NUM_RAFT_SERVERS))
	relaunchGivenRaftServer(t, strconv.Itoa((nextServerToTry+3)%NUM_RAFT_SERVERS))
	relaunchGivenRaftServer(t, strconv.Itoa((nextServerToTry+4)%NUM_RAFT_SERVERS))
	relaunchGivenRaftServer(t, strconv.Itoa((nextServerToTry+5)%NUM_RAFT_SERVERS))
	//give time for relaunch and let it be stable
	time.Sleep(20 * time.Second)
}

/*
	Test the commited logs should survive after failed nodes rejoin
	1. Fail some nodes
	2. Make some set requests
	3. Re-launch those failed nodes
	3. Make get requests, should get back what we set
*/
func TestCommitedLogsShouldSurviveAfterFailedNodesRejoin(t *testing.T) {
	failedNodes := []string{"0", "1"}
	testCommitedLogsShouldSurviveAfterRejoin(t, failedNodes, "test_failed_node_rejoin", "4")
}

/*
	Test the commited logs should survived after failed leader rejoin
	1. Fail the leader
	2. Make some set requests
	3. Re-launch the failed leader
	3. Make get requests, should get back what we set
*/
func TestCommitedLogsShouldSurviveAfterFailedLeaderRejoin(t *testing.T) {
	leaderId := getCurrentLeaderIDByGetRequest(t)
	failedNodes := []string{leaderId}
	testCommitedLogsShouldSurviveAfterRejoin(t, failedNodes, "test_failed_leader_rejoin", "5")
}

func testCommitedLogsShouldSurviveAfterRejoin(t *testing.T, failedNodesList []string, key string, val string) {
	for _, node := range failedNodesList {
		failGivenRaftServer(t, node)
	}
	time.Sleep(20 * time.Second)

	_, kvc := getKVConnectionToRaftLeader(t)
	fireSetRequest(t, kvc, key, val, nil, true)

	for _, node := range failedNodesList {
		relaunchGivenRaftServer(t, node)
	}
	time.Sleep(20 * time.Second)

	_, kvc = getKVConnectionToRaftLeader(t)
	fireGetRequest(t, kvc, key, val, nil, true)
}

/*
	If the failing node(s) is not the leader, and not more than f nodes failed,
	the leader should be able to process requests seamlessly and respond to client.
*/
func TestRequestHandlingDuringNonLeaderFailures(t *testing.T) {
	leaderId, kvc := getKVConnectionToRaftLeader(t)
	t.Logf(leaderId)
	testConcurrentSet(t, kvc, false)

	//fail f Raft servers
	//here we starts 5 servers, f = 2
	var nextServerToTry int
	nextServerToTry, _ = strconv.Atoi(leaderId)
	failGivenRaftServer(t, strconv.Itoa((nextServerToTry+1)%NUM_RAFT_SERVERS))
	failGivenRaftServer(t, strconv.Itoa((nextServerToTry+2)%NUM_RAFT_SERVERS))

	testConcurrentGet(t, kvc, false)

	time.Sleep(20 * time.Second)
	//re-launch the previously killed server for other tests
	relaunchGivenRaftServer(t, strconv.Itoa((nextServerToTry+1)%NUM_RAFT_SERVERS))
	relaunchGivenRaftServer(t, strconv.Itoa((nextServerToTry+2)%NUM_RAFT_SERVERS))
	//give time for relaunch and let it be stable
	time.Sleep(20 * time.Second)
}

/*
	Test a serial of requests and
	the requests should be producing results as expected in the requests firing ordering, as no concurrency is here.

	The corresponding linerizability test case by porcupine is
	- TestRaftKv1ClientSequential
*/
func TestSerialRequestsCorrectness(t *testing.T) {
	f, err := os.Create("raft_test_data/c1-sequential.txt")
	check(err)
	defer f.Close()
	w := &LockedWriter{writer: bufio.NewWriter(f)}

	_, kvc := getKVConnectionToRaftLeader(t)

	fireSetRequest(t, kvc, "x", "1", w, true)
	fireSetRequest(t, kvc, "y", "2", w, true)

	fireGetRequest(t, kvc, "x", "1", w, true)

	fireSetRequest(t, kvc, "x", "2", w, true)
	fireSetRequest(t, kvc, "z", "3", w, true)
	fireSetRequest(t, kvc, "x", "3", w, true)
	fireSetRequest(t, kvc, "y", "4", w, true)

	fireGetRequest(t, kvc, "y", "4", w, true)
	fireGetRequest(t, kvc, "x", "3", w, true)

	fireSetRequest(t, kvc, "z", "3", w, true)

	fireCasRequest(t, kvc, "z", "4", "4", "3", w, true)
	fireCasRequest(t, kvc, "x", "5", "3", "4", w, true)

	fireGetRequest(t, kvc, "x", "3", w, true)
	fireGetRequest(t, kvc, "y", "4", w, true)
	fireGetRequest(t, kvc, "z", "4", w, true)

	w.writer.Flush()
}

/*
	Simple concurrently firing requests, to make sure it at leasts work without errors.
	No lineralizability test yet.
*/
func TestConcurrentRequests(t *testing.T) {
	_, kvc := getKVConnectionToRaftLeader(t)
	testConcurrentSet(t, kvc, true)
	testConcurrentGet(t, kvc, true)
}

/*
	Test concurrent set requests firing to the raft leader
*/
func testConcurrentSet(t *testing.T, kvc pb.KvStoreClient, parallel bool) {
	tc := []struct {
		key string
		val string
	}{
		{"hello", "hi"},
		{"test_f_nodes_failure", "3"},
		{"test_leader_failure", "2"},
		{"abc", "def"},
		{"abcde", "fgh"},
		{"ywn202", "Yik Wai Ng"},
		{"nyu", "New York University"},
	}

	for _, tt := range tc {
		tt := tt
		t.Run("ConcurrentSet", func(st *testing.T) {
			if parallel {
				st.Parallel()
			}
			fireSetRequest(t, kvc, tt.key, tt.val, nil, true)
		})
	}
}

/*
	Test concurrent get requests firing to the raft leader
*/
func testConcurrentGet(t *testing.T, kvc pb.KvStoreClient, parallel bool) {
	tc := []struct {
		key string
		val string
	}{
		{"hello", "hi"},
		{"test_f_nodes_failure", "3"},
		{"test_leader_failure", "2"},
		{"abc", "def"},
		{"hello", "hi"},
		{"abc", "def"},
		{"test_f_nodes_failure", "3"},
		{"test_leader_failure", "2"},
	}

	for _, tt := range tc {
		tt := tt
		t.Run("ConcurrentGet", func(st *testing.T) {
			if parallel {
				st.Parallel()
			}
			fireGetRequest(t, kvc, tt.key, tt.val, nil, true)
		})
	}
}

/*
	A mix of concurrent get, set, cas without expected value check/assertion.
	We will do the linerizability check if it is correct.

	The corresponding linerizability test case by porcupine is
	- TestRaftKvManyClientConcurrentGetSetCas
*/
func TestConcurrentGetSetCas(t *testing.T) {
	f, err := os.Create("raft_test_data/c-many-concurrent-get-set-cas.txt")
	check(err)
	defer f.Close()
	w := &LockedWriter{writer: bufio.NewWriter(f)}

	r := rand.Intn(13)

	_, kvc := getKVConnectionToRaftLeader(t)

	fireClearRequest(t, kvc)

	tc := []struct {
		op     int //0: get, 1:set, 2:cas
		key    string
		val    string
		oldVal string
	}{
		{1, "hello", "hi", ""},
		{1, "test_f_nodes_failure", "3", ""},
		{1, "test_leader_failure", "2", ""},
		{1, "abc", "def", ""},
		{0, "test_f_nodes_failure", "", ""},
		{0, "hello", "", ""},
		{1, "nyu", "New New York University", ""},
		{0, "test_f_nodes_failure", "", ""},
		{0, "hello", "", ""},
		{2, "abc", "hig", "def"},
		{1, "OOP", "Object Oriented Programming", ""},
		{2, "abc", "dwdwdw", "dwdw"},
		{1, "test_f_nodes_failure", "9", ""},
		{1, "abc", "def", ""},
		{0, "test_f_nodes_failure", "", ""},
		{0, "hello", "", ""},
		{2, "nyu", "hig", "New New York University"},
		{1, "test_f_nodes_failure", "9", ""},
		{0, "OOP", "", ""},
		{0, "test_f_nodes_failure", "", ""},
		{2, "test_leader_failure", "8", "7"},
		{1, "abcde", "defee", ""},
		{0, "nyu", "", ""},
		{2, "nyu", "what is it?", "New New York University"},
		{0, "nyuabc", "", ""},
		{2, "abc", "higdwdwdw", "defwwww"},
		{0, "nyu", "", ""},
		{1, "test_f_nodes_failure", "9", ""},
	}

	var wg sync.WaitGroup
	wg.Add(len(tc))

	for _, tt := range tc {
		tt := tt
		time.Sleep(time.Duration(r) * time.Millisecond)
		switch tt.op {
		case 0:
			go func() {
				defer wg.Done()
				fireGetRequest(t, kvc, tt.key, tt.val, w, false)
			}()
		case 1:
			go func() {
				defer wg.Done()
				fireSetRequest(t, kvc, tt.key, tt.val, w, false)
			}()
		case 2:
			go func() {
				defer wg.Done()
				fireCasRequest(t, kvc, tt.key, tt.val, "", tt.oldVal, w, false)
			}()
		}
	}

	wg.Wait()
	w.writer.Flush()
}

/*
	Concurrent requests of high contention,
	simulate several clients are making exactly the same set of requests concurrently.
	We will do the linerizability check if it is correct.

	The corresponding linerizability test case by porcupine is
	- TestRaftKvHighConcurrentContention
*/
func TestHighConcurrentContention(t *testing.T) {
	num_concurr_threads := 10
	t.Logf("Number of concurrent threads: %v", num_concurr_threads)

	f, err := os.Create("raft_test_data/c-high-concurrent-contention.txt")
	check(err)
	defer f.Close()
	w := &LockedWriter{writer: bufio.NewWriter(f)}

	r := rand.Intn(15)

	_, kvc := getKVConnectionToRaftLeader(t)

	fireClearRequest(t, kvc)

	tc := []struct {
		op     int //0: get, 1:set, 2:cas
		key    string
		val    string
		oldVal string
	}{
		{1, "hello", "hi", ""},
		{1, "test_f_nodes_failure", "3", ""},
		{1, "test_leader_failure", "2", ""},
		{1, "abc", "def", ""},
		{0, "test_f_nodes_failure", "", ""},
		{0, "hello", "", ""},
		{1, "nyu", "New New York University", ""},
		{0, "test_f_nodes_failure", "", ""},
		{0, "hello", "", ""},
		{0, "OOP", "", ""},
		{0, "nyu", "", ""},
		{1, "OOP", "Object Oriented Programming", ""},
		{1, "test_f_nodes_failure", "9", ""},
		{1, "abc", "defdsds", ""},
		{1, "hello", "hihi???", ""},
		{1, "test_f_nodes_failure", "4", ""},
		{1, "test_leader_failure", "8", ""},
		{0, "test_f_nodes_failure", "", ""},
		{0, "hello", "", ""},
		{1, "test_f_nodes_failure", "9d0", ""},
		{0, "OOP", "", ""},
		{0, "test_f_nodes_failure", "", ""},
		{1, "abcde", "defee", ""},
		{0, "nyu", "", ""},
		{1, "nyu", "New New York University? seriously", ""},
		{0, "nyuabc", "", ""},
		{0, "nyu", "", ""},
		{1, "test_f_nodes_failure", "91", ""},
	}

	var wg sync.WaitGroup
	wg.Add(num_concurr_threads) //simulate 5 clients making the same set of requests concurrently

	for i := 1; i <= num_concurr_threads; i++ {
		go func() {
			time.Sleep((time.Duration(r) + 1) * time.Millisecond)
			defer wg.Done()
			//each simulated client fires the same set of requests
			for _, tt := range tc {
				tt := tt
				time.Sleep(time.Duration(r) * time.Millisecond)
				switch tt.op {
				case 0:
					fireGetRequest(t, kvc, tt.key, tt.val, w, false)
				case 1:
					fireSetRequest(t, kvc, tt.key, tt.val, w, false)
				case 2:
					fireCasRequest(t, kvc, tt.key, tt.val, "", tt.oldVal, w, false)
				}
			}
		}()
	}

	wg.Wait()
	w.writer.Flush()
}

/*
	Concurrent requests at the same time with nodes failure happening (<= f)
	We will do the linerizability check if it is correct.

	The corresponding linerizability test case by porcupine is
	- TestRaftKvConcurrentDuringNodeFailure
*/
func TestConcurrentDuringNodeFailure(t *testing.T) {
	f, err := os.Create("raft_test_data/c-concurrent-during-node-failure.txt")
	check(err)
	defer f.Close()
	w := &LockedWriter{writer: bufio.NewWriter(f)}

	r := rand.Intn(15)

	leaderId, kvc := getKVConnectionToRaftLeader(t)

	fireClearRequest(t, kvc)

	tc := []struct {
		op     int //0: get, 1:set, 2:cas
		key    string
		val    string
		oldVal string
	}{
		{1, "hello", "hi", ""},
		{1, "test_f_nodes_failure", "3", ""},
		{0, "test_f_nodes_failure", "", ""},
		{0, "hello", "", ""},
		{0, "OOP", "", ""},
		{0, "nyu", "", ""},
		{1, "OOP", "Object Oriented Programming", ""},
		{1, "test_f_nodes_failure", "9", ""},
		{1, "abc", "defdsds", ""},
		{1, "hello", "hihi???", ""},
		{1, "test_f_nodes_failure", "4", ""},
		{1, "test_f_nodes_failure", "9d0", ""},
		{0, "OOP", "", ""},
		{0, "test_f_nodes_failure", "", ""},
		{1, "abcde", "defee", ""},
		{0, "nyu", "", ""},
		{1, "nyu", "New New York University? seriously", ""},
		{0, "nyuabc", "", ""},
		{0, "nyu", "", ""},
	}

	var wg sync.WaitGroup
	wg.Add(10) //simulate 5 clients making the same set of requests concurrently

	var nextServerToTry int
	nextServerToTry, _ = strconv.Atoi(leaderId)

	for i := 1; i <= 10; i++ {
		i := i
		go func() {
			time.Sleep(time.Duration(r) * time.Millisecond)
			if i <= 5 {
				failGivenRaftServer(t, strconv.Itoa((nextServerToTry+i)%NUM_RAFT_SERVERS))
			}
			defer wg.Done()
			//each simulated client fires the same set of requests
			for _, tt := range tc {
				tt := tt
				switch tt.op {
				case 0:
					fireGetRequest(t, kvc, tt.key, tt.val, w, false)
				case 1:
					fireSetRequest(t, kvc, tt.key, tt.val, w, false)
				case 2:
					fireCasRequest(t, kvc, tt.key, tt.val, "", tt.oldVal, w, false)
				}
			}
		}()
	}

	wg.Wait()
	w.writer.Flush()

	time.Sleep(60 * time.Second)
	//restart the failed server for other testing
	for i := 1; i <= 5; i++ {
		relaunchGivenRaftServer(t, strconv.Itoa((nextServerToTry+i)%NUM_RAFT_SERVERS))
	}
	time.Sleep(30 * time.Second)
}
