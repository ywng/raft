package main

import (
	"bytes"
	"encoding/gob"
	"log"

	context "golang.org/x/net/context"

	"github.com/raft/pb"
)

// The struct for data to send over channel
type InputChannelType struct {
	command  pb.Command
	response chan pb.Result
}

// The struct for key value stores.
type KVStore struct {
	C     chan InputChannelType
	store map[string]string
}

func (s *KVStore) Get(ctx context.Context, key *pb.Key) (*pb.Result, error) {
	// Create a channel
	c := make(chan pb.Result)
	// Create a request
	r := pb.Command{Operation: pb.Op_GET, Arg: &pb.Command_Get{Get: key}}
	// Send request over the channel
	s.C <- InputChannelType{command: r, response: c}
	//log.Printf("Waiting for get response")
	result := <-c
	// The bit below works because Go maps return the 0 value for non existent keys, which is empty in this case.
	return &result, nil
}

func (s *KVStore) Set(ctx context.Context, in *pb.KeyValue) (*pb.Result, error) {
	// Create a channel
	c := make(chan pb.Result)
	// Create a request
	r := pb.Command{Operation: pb.Op_SET, Arg: &pb.Command_Set{Set: in}}
	// Send request over the channel
	s.C <- InputChannelType{command: r, response: c}
	//log.Printf("Waiting for set response")
	result := <-c
	// The bit below works because Go maps return the 0 value for non existent keys, which is empty in this case.
	return &result, nil
}

func (s *KVStore) Clear(ctx context.Context, in *pb.Empty) (*pb.Result, error) {
	// Create a channel
	c := make(chan pb.Result)
	// Create a request
	r := pb.Command{Operation: pb.Op_CLEAR, Arg: &pb.Command_Clear{Clear: in}}
	// Send request over the channel
	s.C <- InputChannelType{command: r, response: c}
	//log.Printf("Waiting for clear response")
	result := <-c
	// The bit below works because Go maps return the 0 value for non existent keys, which is empty in this case.
	return &result, nil
}

func (s *KVStore) CAS(ctx context.Context, in *pb.CASArg) (*pb.Result, error) {
	// Create a channel
	c := make(chan pb.Result)
	// Create a request
	r := pb.Command{Operation: pb.Op_CAS, Arg: &pb.Command_Cas{Cas: in}}
	// Send request over the channel
	s.C <- InputChannelType{command: r, response: c}
	//log.Printf("Waiting for CAS response")
	result := <-c
	// The bit below works because Go maps return the 0 value for non existent keys, which is empty in this case.
	return &result, nil
}

func (s *KVStore) ChangeConfiguration(ctx context.Context, in *pb.Servers) (*pb.Result, error) {
	// Create a channel
	c := make(chan pb.Result)
	// Create a request
	r := pb.Command{Operation: pb.Op_CONFIG_CHG, Arg: &pb.Command_Servers{Servers: in}}
	// Send request over the channel
	s.C <- InputChannelType{command: r, response: c}
	//log.Printf("Waiting for CAS response")
	result := <-c
	// The bit below works because Go maps return the 0 value for non existent keys, which is empty in this case.
	return &result, nil
}

// Used internally to generate a result for a get request. This function assumes that it is called from a single thread of
// execution, and hence does not handle races.
func (s *KVStore) GetInternal(k string) pb.Result {
	v := s.store[k]
	return pb.Result{Result: &pb.Result_Kv{Kv: &pb.KeyValue{Key: k, Value: v}}}
}

// Used internally to set and generate an appropriate result. This function assumes that it is called from a single
// thread of execution and hence does not handle race conditions.
func (s *KVStore) SetInternal(k string, v string) pb.Result {
	s.store[k] = v
	return pb.Result{Result: &pb.Result_Kv{Kv: &pb.KeyValue{Key: k, Value: v}}}
}

// Used internally, this function clears a kv store. Assumes no racing calls.
func (s *KVStore) ClearInternal() pb.Result {
	s.store = make(map[string]string)
	return pb.Result{Result: &pb.Result_S{S: &pb.Success{}}}
}

// Used internally this function performs CAS assuming no races.
func (s *KVStore) CasInternal(k string, v string, vn string) pb.Result {
	vc := s.store[k]
	if vc == v {
		s.store[k] = vn
		return pb.Result{Result: &pb.Result_Kv{Kv: &pb.KeyValue{Key: k, Value: vn}}}
	} else {
		return pb.Result{Result: &pb.Result_Kv{Kv: &pb.KeyValue{Key: k, Value: vc}}}
	}
}

func (s *KVStore) HandleCommand(op InputChannelType) {
	log.Printf("kv-store is handling committed command: %s", op.command.Operation)

	var result pb.Result
	var unrecognizedOp bool = false

	switch c := op.command; c.Operation {
	case pb.Op_GET:
		arg := c.GetGet()
		result = s.GetInternal(arg.Key)
	case pb.Op_SET:
		arg := c.GetSet()
		result = s.SetInternal(arg.Key, arg.Value)
	case pb.Op_CLEAR:
		result = s.ClearInternal()
	case pb.Op_CAS:
		arg := c.GetCas()
		result = s.CasInternal(arg.Kv.Key, arg.Kv.Value, arg.Value.Value)
	default:
		// Sending a blank response to just free things up, but we don't know how to make progress here.
		result = pb.Result{}
		unrecognizedOp = true
	}

	//use select to do non-blocking send
	select {
	case op.response <- result:
		log.Printf("kv-store command completed and response is sent to client.")
	default:
		//no response is sent when non-leader is handling the command
		log.Printf("kv-store command completed and no response is sent to client.")
	}

	if unrecognizedOp {
		log.Fatalf("Unrecognized operation %v", op.command.Operation)
	}
}

func (s *KVStore) ApplySnapshot(snapshot []byte) {
	data := bytes.NewBuffer(snapshot)
	decoder := gob.NewDecoder(data)
	decoder.Decode(&s.store)
}
