package chandy_lamport

import (
	"log"
	"math/rand"
	"sync"
)

// Max random delay added to packet delivery
const maxDelay = 5

type ChandyLamportSim struct {
	time              int
	nextSnapshotId    int
	nodes             map[string]*Node // key = node ID
	logger            *Logger
	activeSnapshotsWG map[int]*sync.WaitGroup // key = snapshot ID
	lock              sync.Mutex
}

type LockableSnapshot struct {
	id       int            // snapshot id
	tokenMap map[string]int // key = node ID, value = num tokens
	messages []*MsgSnapshot // snapshots of all messages recorded across all links
	lock     sync.Mutex     // lock to protect the snapshot
}

func NewSimulator() *ChandyLamportSim {
	return &ChandyLamportSim{
		time:              0,
		nextSnapshotId:    0,
		nodes:             make(map[string]*Node),
		logger:            NewLogger(),
		activeSnapshotsWG: make(map[int]*sync.WaitGroup),
		lock:              sync.Mutex{},
	}
}

// Add a node to this simulator with the specified number of starting tokens
func (sim *ChandyLamportSim) AddNode(id string, tokens int) {
	node := CreateNode(id, tokens, sim)
	sim.nodes[id] = node
}

// Add a unidirectional link between two nodes
func (sim *ChandyLamportSim) AddLink(src string, dest string) {
	node1, ok1 := sim.nodes[src]
	node2, ok2 := sim.nodes[dest]
	if !ok1 {
		log.Fatalf("Node %v does not exist\n", src)
	}
	if !ok2 {
		log.Fatalf("Node %v does not exist\n", dest)
	}
	node1.AddOutboundLink(node2)
}

func (sim *ChandyLamportSim) ProcessEvent(event interface{}) {
	switch event := event.(type) {
	case PassTokenEvent:
		src := sim.nodes[event.src]
		src.SendTokens(event.tokens, event.dest)
	case SnapshotEvent:
		sim.StartSnapshot(event.nodeId)
	default:
		log.Fatal("Error unknown event: ", event)
	}
}

// Advance the simulator time forward by one step and deliver at most one packet per node
func (sim *ChandyLamportSim) Tick() {
	sim.time++
	sim.logger.NewEpoch()
	// Note: to ensure deterministic ordering of packet delivery across the nodes,
	// we must also iterate through the nodes and the links in a deterministic way
	for _, nodeId := range getSortedKeys(sim.nodes) {
		node := sim.nodes[nodeId]
		for _, dest := range getSortedKeys(node.outboundLinks) {
			link := node.outboundLinks[dest]
			// Deliver at most one packet per node at each time step to
			// establish total ordering of packet delivery to each node
			if !link.msgQueue.Empty() {
				e := link.msgQueue.Peek().(SendMsgEvent)
				if e.receiveTime <= sim.time {
					link.msgQueue.Pop()
					sim.logger.RecordEvent(
						sim.nodes[e.dest],
						ReceivedMsgRecord{e.src, e.dest, e.message})
					sim.nodes[e.dest].HandlePacket(e.src, e.message)
					break
				}
			}
		}
	}
}

// Return the receive time of a message after adding a random delay.
// Note: At each time step, only one message is delivered to a destination.
// This implies that the message may be received *after* the time step returned in this function.
func (sim *ChandyLamportSim) GetReceiveTime() int {
	return sim.time + 1 + rand.Intn(maxDelay)
}

// Start a snapshot at the specified node
func (sim *ChandyLamportSim) StartSnapshot(nodeId string) {
	node := sim.nodes[nodeId]
	snapshotId := sim.nextSnapshotId
	sim.nextSnapshotId++
	sim.logger.RecordEvent(sim.nodes[nodeId], StartSnapshotRecord{nodeId, snapshotId})

	sim.lock.Lock()
	if sim.activeSnapshotsWG == nil {
		sim.activeSnapshotsWG = make(map[int]*sync.WaitGroup)
	}

	sim.activeSnapshotsWG[snapshotId] = &sync.WaitGroup{}
	sim.activeSnapshotsWG[snapshotId].Add(len(sim.nodes))
	sim.lock.Unlock()

	node.lock.Lock()
	node.StartSnapshot(snapshotId)
	node.lock.Unlock()
}

// Notify the simulator that a snapshot has been completed at the specified node
func (sim *ChandyLamportSim) NotifyCompletedSnapshot(nodeId string, snapshotId int) {
	sim.logger.RecordEvent(sim.nodes[nodeId], EndSnapshotRecord{nodeId, snapshotId})
	sim.lock.Lock()
	sim.activeSnapshotsWG[snapshotId].Done()
	sim.lock.Unlock()
}

// Collect the global snapshot across all nodes
func (sim *ChandyLamportSim) CollectSnapshot(snapshotId int) *GlobalSnapshot {

	// Wait for all nodes to complete their snapshots
	sim.lock.Lock()
	globalWG := sim.activeSnapshotsWG[snapshotId]
	sim.lock.Unlock()
	globalWG.Wait()

	// Prepare to collect the local snapshots from all nodes
	snapshotCollectionWG := sync.WaitGroup{}
	tempSnap := LockableSnapshot{snapshotId, make(map[string]int), make([]*MsgSnapshot, 0), sync.Mutex{}}

	for _, node := range sim.nodes {
		snapshotCollectionWG.Add(1)
		go func(node *Node) {
			defer snapshotCollectionWG.Done()

			node.lock.Lock()
			localWG := node.activeSnapshots[snapshotId].wg
			node.lock.Unlock()
			localWG.Wait()

			node.lock.Lock()
			localSnap := node.activeSnapshots[snapshotId]
			node.lock.Unlock()

			// Gather the local snapshot information in a thread-safe manner
			tempSnap.lock.Lock()
			tempSnap.tokenMap[node.id] = localSnap.numTokensInNode
			tempSnap.messages = append(tempSnap.messages, localSnap.msgSnapshots...)
			tempSnap.lock.Unlock()
		}(node)
	}

	// Wait for all local snapshots to be collected
	snapshotCollectionWG.Wait()

	globalSnap := GlobalSnapshot{snapshotId, tempSnap.tokenMap, tempSnap.messages}
	return &globalSnap
}
