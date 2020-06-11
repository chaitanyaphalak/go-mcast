package mcast

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// Err is returned when an RPC arrives in a version that the current
	// unity cannot handle.
	ErrUnsupportedProtocol = errors.New("protocol version not supported")
)

// Holds information for shutting down the whole group.
type poweroff struct {
	shutdown bool
	ch       chan struct{}
	mutex    *sync.Mutex
}

// Holds information for management
type contextHolder struct {
	started bool
	mutex   *sync.Mutex
}

// Unity is a group
type Unity struct {
	// Local peer id
	id ServerID

	// Unity context information
	context contextHolder

	// Hold information for the group, the group acts like a unity.
	// The unity *must* have a majority of non-faulty members.
	state *GroupState

	// PreviousSet is the protocol needed for conflict evaluation.
	previousSet *PreviousSet

	// Holds configuration about the unity. About the local group name,
	// logger utilities, protocol version, etc.
	configuration *BaseConfiguration

	// The unity state machine to commit values.
	sm StateMachine

	// The global clock that can be used to synchronize groups.
	clock LogicalGlobalClock

	// Provided from the transport
	channel <-chan RPC

	// Transport layer for communication.
	trans Transport

	// Storage for storing information about the state machine.
	storage Storage

	// Shutdown channel to exit, protected to prevent concurrent exits.
	off poweroff
}

func NewUnity(base *BaseConfiguration, cluster *ClusterConfiguration, storage Storage, clock LogicalGlobalClock) (*Unity, error) {
	state, err := BootstrapGroup(base, cluster)
	if err != nil {
		return nil, err
	}

	node := state.Nodes[0]
	ctx := contextHolder{
		started: false,
		mutex:   &sync.Mutex{},
	}
	off := poweroff{
		shutdown: false,
		ch:       make(chan struct{}),
		mutex:    &sync.Mutex{},
	}
	state.group = &sync.WaitGroup{}
	unity := &Unity{
		id:            node.Server.ID,
		context:       ctx,
		state:         state,
		previousSet:   NewPreviousSet(),
		configuration: base,
		sm:            nil,
		clock:         clock,
		channel:       nil,
		trans:         node.Trans,
		storage:       storage,
		off:           off,
	}

	unity.state.emit(unity.run)

	return unity, nil
}

// Creates an RPC header to be sent across RPC requests.
func (u *Unity) getRPCHeader() RPCHeader {
	return RPCHeader{
		ProtocolVersion: u.configuration.Version,
	}
}

// Verify if the current version can handle the current RPC request.
func (u *Unity) checkRPCHeader(rpc RPC) error {
	h, ok := rpc.Command.(WithRPCHeader)
	if !ok {
		return fmt.Errorf("RPC doest not have a header")
	}

	header := h.GetRPCHeader()
	if header.ProtocolVersion > LatestProtocolVersion {
		return ErrUnsupportedProtocol
	}

	if header.ProtocolVersion != u.configuration.Version {
		return ErrUnsupportedProtocol
	}

	return nil
}

func (u *Unity) run() {
	for {
		select {
		case <-u.off.ch:
			// handle poweroff
			return
		default:
		}

		if !u.context.started && !u.off.shutdown {
			u.context.started = true
			u.poll()
		}
	}
}

func (u *Unity) poll() {
	// Handle clean up when the node gives up and shutdown
	defer func() {
		u.configuration.Logger.Infof("shutdown process %s", u.id)
	}()

	for !u.off.shutdown {
		select {
		case rpc := <-u.channel:
			u.process(rpc)
		case <-u.off.ch:
			return
		}
	}
}

func (u *Unity) process(rpc RPC) {
	// Verify if the current unity is able to process
	// the rpc that just arrives.
	if err := u.checkRPCHeader(rpc); err != nil {
		return
	}

	switch cmd := rpc.Command.(type) {
	case *GMCastRequest:
		u.processGMCast(rpc, cmd)
	default:
		u.configuration.Logger.Errorf("unexpected command: %#v", rpc.Command)
	}
}

// Will process the received GM-Cast rpc request.
func (u *Unity) processGMCast(rpc RPC, r *GMCastRequest) {
	res := &GMCastResponse{
		RPCHeader:      u.getRPCHeader(),
		SequenceNumber: u.state.Clk.Tock(),
		Success:        false,
	}

	var rpcErr error
	defer func() {
		rpc.Respond(res, rpcErr)
	}()

	computeChannel := u.handleGMCast(r, r.Body)
	quorum := u.unityQuorum()
	votes := 0
	done := false

	computedTimestamps := make([]uint64, 0)

	select {
	case newRpc := <-u.channel:
		u.process(newRpc)

	case computed := <-computeChannel:
		votes++
		if votes >= quorum && !done {
			done = true
			switch computed.State {
			case S1:
			case S2:
				// There is more than one process group on the destination, need to execute
				// a gather request to exchange the timestamp between groups.
				computedTimestamps = append(computedTimestamps, computed.Timestamp)
				if votes >= quorum {
					tsm := max(computedTimestamps)
					gatherReq := &GatherRequest{
						RPCHeader: u.getRPCHeader(),
						UID:       r.UID,
						State:     computed.State,
						Timestamp: tsm,
					}
					sequenceNumber := u.emitGather(gatherReq)
					u.configuration.Logger.Infof("sequence number is %ld", sequenceNumber)
				}
			case S3:
				// Ready to be committed into the state machine.
			default:
				u.configuration.Logger.Error("unknown compute response state %#v", computed)
			}
		}
	}

}

// Will handle the received GM-cast issuing the needed RPC requests.
// Each process that receives the message m, set the state of m to S0,
// this indicates that m don't have a timestamp associated yet. Then the
// message is broadcast for the processes groups.
func (u *Unity) handleGMCast(r *GMCastRequest, message Message) <-chan *ComputeResponse {
	channel := make(chan *ComputeResponse, len(u.state.Nodes))
	req := &ComputeRequest{
		RPCHeader:   u.getRPCHeader(),
		UID:         r.UID,
		State:       message.MessageState,
		Destination: r.Destination,
	}

	ask := func(peer Server) {
		u.state.emit(func() {
			res := &ComputeResponse{}
			err := u.trans.Compute(peer.ID, peer.Address, req, res)
			if err != nil {
				u.configuration.Logger.Errorf("failed on compute RPC to target %#v. error %v", peer, err)
				res.State = S0
			}
			channel <- res
		})
	}

	for _, node := range u.state.Nodes {
		// Is possible to avoid this call if the target
		// node id is the same as the local id, and just process
		// the request locally. Is the best option?
		ask(node.Server)
	}

	return channel
}

func (u *Unity) emitGather(req *GatherRequest) uint64 {
	channel := make(chan *GatherResponse, len(req.Destination))
	emit := func(peer Server) {
		u.state.emit(func() {
			res := new(GatherResponse)
			err := u.trans.Gather(peer.ID, peer.Address, req, res)
			if err != nil {
				u.configuration.Logger.Errorf("failed to gather to target %v. error %v", peer, err)
			}
			channel <- res
		})
	}

	received := 0
	var tsm uint64
	select {
	case res := <-channel:
		received++
		if received == len(req.Destination) {
			tsm = res.Timestamp
		}
	case <-time.After(5 * time.Second):
		// fixme: returns error on timeout
		panic("timeout gathering")
	}

	for _, destination := range req.Destination {
		emit(destination)
	}

	return tsm
}

// Process Compute requests, after the messages is broadcast to the process groups.
// First is verified if the message m conflicts with any other message on the unity
// previous set, if so, the process p increments the local clock and clear the previous set.
// Then m receives a timestamp and is added to the previous set, for future verifications.
// If m has only one destination, the message can jump to state S3, since there is no need
// to exchange the timestamp message group, that is already decided.
//
// Otherwise, for messages on state S0 the timestamp received is the local group clock,
// updates the state to S1 and exchange the message with all processes. If the message is
// already on state S2, it already have the final timestamp and just needs to update m to
// the final timestamp, update the group clock and clear the previous set.
func (u *Unity) processCompute(rpc RPC, r *ComputeRequest) {
	res := &ComputeResponse{
		RPCHeader: u.getRPCHeader(),
		UID:       r.UID,
	}
	var rpcError error

	defer func() {
		rpc.Respond(res, rpcError)
	}()

	if r.State == S0 {
		var addresses []ServerAddress
		for _, v := range r.Destination {
			addresses = append(addresses, v.Address)
		}
		if u.previousSet.Conflicts(addresses) {
			u.state.Clk.Tick()
			u.previousSet.Clear()
		}
		r.Timestamp = u.state.Clk.Tock()
		u.previousSet.Add(addresses, r.UID)
	}

	if len(r.Destination) > 1 {
		// On state S0, the message is receiving its first timestamp definition
		// the local group clock will define the message timestamp that will be
		// answered back, all answers will be grouped and the next step can be executed.
		if r.State == S0 {
			res.State = S1
			res.Timestamp = u.state.Clk.Tock()
		} else if r.State == S2 {
			res.State = S3
			if r.Timestamp > u.state.Clk.Tock() {
				u.state.Clk.Leap(r.Timestamp)
				u.previousSet.Clear()
			}
			res.Timestamp = u.state.Clk.Tock()
		}
	} else {
		res.Timestamp = u.state.Clk.Tock()
		res.State = S3
	}
}

// Process a Gather request.
// When a message m has more than on destination group, the destination groups have
// to exchange its timestamps to decide the final timestamp on m. Thus, after
// receiving all other timestamp values, a temporary variable tsm is agreed upon the
// maximum timestamp value received. Once the algorithm have selected the tsm value,
// the process checks if the global consensus timestamp is greater or equal to tsm.
//
// The computed tsm will be already coupled into the GatherRequest.
func (u *Unity) processGather(rpc RPC, r *GatherRequest) {
	res := &GatherResponse{
		RPCHeader: u.getRPCHeader(),
		UID:       r.UID,
	}
	defer rpc.Respond(res, nil)

	if r.Timestamp >= u.clock.Tock() {
		res.State = S3
	} else {
		res.Timestamp = u.clock.Tock()
		res.State = S2
	}
}

// How many local nodes must reply to quorum is obtained.
func (u *Unity) unityQuorum() int {
	return len(u.state.Nodes)/2 + 1
}

// Shutdown all current spawned goroutines and returns
// a blocking future to wait for the complete shutdown.
func (u *Unity) Shutdown() Future {
	u.off.mutex.Lock()
	defer u.off.mutex.Unlock()

	if !u.off.shutdown {
		close(u.off.ch)
		u.off.shutdown = true
		return &ShutdownFuture{unity: u}
	}
	return &ShutdownFuture{unity: nil}
}

func max(values []uint64) uint64 {
	var v uint64
	for _, e := range values {
		if e > v {
			v = e
		}
	}
	return v
}
