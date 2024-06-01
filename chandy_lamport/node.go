package chandy_lamport

import (
	"log"
	"sync"
)

// The main participant of the distributed snapshot protocol.
// nodes exchange token messages and marker messages among each other.
// Token messages represent the transfer of tokens from one node to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.

type Node struct {
	sim             *ChandyLamportSim
	id              string
	tokens          int
	outboundLinks   map[string]*Link       // key = link.dest
	inboundLinks    map[string]*Link       // key = link.src
	activeSnapshots map[int]*LocalSnapshot // key = snapshot ID, value = local snapshot state
	lock            sync.Mutex
}

// A unidirectional communication channel between two nodes
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src      string
	dest     string
	msgQueue *Queue
}

// A snapshot of the local state of a node at a given time
// This struct is used to store the local state of a node at the time of a snapshot
type LocalSnapshot struct {
	id                    int                  // snapshot id
	owner                 string               // node ID of the node that initiated the snapshot
	numTokensInNode       int                  // number of tokens held by the node at the time of the snapshot
	incomingMessages      map[string][]Message // key = link.src, value = messages received from the link at the time of the snapshot
	isLinkRecording       map[string]bool      // key = link.src, value = whether the link was being recorded
	numLinksBeingRecorded int                  // counter to keep track of the number of markers received
	wg                    *sync.WaitGroup      // wait group to keep track of the number of markers received
	msgSnapshots          []*MsgSnapshot       // snapshots of all messages recorded across all links
}

func CreateNode(id string, tokens int, sim *ChandyLamportSim) *Node {
	return &Node{
		sim:             sim,
		id:              id,
		tokens:          tokens,
		outboundLinks:   make(map[string]*Link),
		inboundLinks:    make(map[string]*Link),
		activeSnapshots: make(map[int]*LocalSnapshot),
		lock:            sync.Mutex{},
	}
}

// Create a new local snapshot for the node
func (node *Node) CreateLocalSnapshot(snapshotId int, srcLink string) {

	// Initialize the local snapshot state
	numLinksToRecord := len(node.inboundLinks)
	recordedLinks := make(map[string]bool)
	for _, link := range node.inboundLinks {
		recordedLinks[link.src] = true
	}
	if srcLink != "" {
		recordedLinks[srcLink] = false
		numLinksToRecord = len(node.inboundLinks) - 1
	}
	localWaitGroup := &sync.WaitGroup{}
	localWaitGroup.Add(numLinksToRecord)

	// Create the local snapshot
	node.activeSnapshots[snapshotId] = &LocalSnapshot{
		id:                    snapshotId,
		owner:                 node.id,
		numTokensInNode:       node.tokens,
		incomingMessages:      make(map[string][]Message),
		isLinkRecording:       recordedLinks,
		numLinksBeingRecorded: numLinksToRecord,
		wg:                    localWaitGroup,
		msgSnapshots:          make([]*MsgSnapshot, 0),
	}
}

// Add a unidirectional link to the destination node
func (node *Node) AddOutboundLink(dest *Node) {
	if node == dest {
		return
	}
	l := Link{node.id, dest.id, NewQueue()}
	node.outboundLinks[dest.id] = &l
	dest.inboundLinks[node.id] = &l
}

// Send a message on all of the node's outbound links
func (node *Node) SendToNeighbors(message Message) {
	for _, nodeId := range getSortedKeys(node.outboundLinks) {
		link := node.outboundLinks[nodeId]
		node.sim.logger.RecordEvent(
			node,
			SentMsgRecord{node.id, link.dest, message})
		link.msgQueue.Push(SendMsgEvent{
			node.id,
			link.dest,
			message,
			node.sim.GetReceiveTime()})
	}
}

// Send a number of tokens to a neighbor attached to this node
func (node *Node) SendTokens(numTokens int, dest string) {
	if node.tokens < numTokens {
		log.Fatalf("node %v attempted to send %v tokens when it only has %v\n",
			node.id, numTokens, node.tokens)
	}
	message := Message{isMarker: false, data: numTokens}
	node.sim.logger.RecordEvent(node, SentMsgRecord{node.id, dest, message})
	// Update local state before sending the tokens
	node.tokens -= numTokens
	link, ok := node.outboundLinks[dest]
	if !ok {
		log.Fatalf("Unknown dest ID %v from node %v\n", dest, node.id)
	}

	link.msgQueue.Push(SendMsgEvent{
		node.id,
		dest,
		message,
		node.sim.GetReceiveTime()})
}

// Send a marker message to a neighbor attached to this node.
func (node *Node) SendMarker(snapshotId int, dest string) {
	message := Message{isMarker: true, data: snapshotId}
	node.sim.logger.RecordEvent(node, SentMsgRecord{node.id, dest, message})
}

// Handle a packet received by the node.
func (node *Node) HandlePacket(src string, message Message) {
	if message.isMarker {
		node.HandleMarker(src, message)
	} else {
		node.HandleToken(src, message)
	}
}

// Handle a marker message received by the node.
func (node *Node) HandleMarker(src string, message Message) {
	var snapshotID int = message.data

	node.lock.Lock()
	// If the node has not yet started a snapshot, it will create a new snapshot.
	if node.activeSnapshots[snapshotID] == nil {
		node.CreateLocalSnapshot(snapshotID, src)
		node.StartSnapshot(snapshotID)
	} else { // If the node has already started a snapshot, it will stop recording on the link.
		node.activeSnapshots[snapshotID].isLinkRecording[src] = false
		node.activeSnapshots[snapshotID].numLinksBeingRecorded--
		node.activeSnapshots[snapshotID].wg.Done()
	}
	node.lock.Unlock()

	// If the node has finished recording on all links, it will finalize the snapshot.
	if node.activeSnapshots[snapshotID].numLinksBeingRecorded == 0 {
		node.lock.Lock()
		node.finalizeSnapshot(snapshotID)
		node.lock.Unlock()
		node.sim.NotifyCompletedSnapshot(node.id, snapshotID)
	}
}

// Handle a token message received by the node.
func (node *Node) HandleToken(src string, message Message) {
	node.tokens += message.data

	// For all currently active snapshots, record the message if the link is being recorded.
	node.lock.Lock()
	for _, snapshot := range node.activeSnapshots {
		if snapshot.isLinkRecording[src] {
			snapshot.incomingMessages[src] = append(snapshot.incomingMessages[src], message)
		}
	}
	node.lock.Unlock()
}

// Finalize the snapshot by recording all messages received on the links into MsgSnapshot structs.
func (node *Node) finalizeSnapshot(snapshotId int) {
	snapshot := node.activeSnapshots[snapshotId]
	for src, messages := range snapshot.incomingMessages {
		for i := 0; i < len(messages); i++ {
			node.activeSnapshots[snapshotId].msgSnapshots = append(node.activeSnapshots[snapshotId].msgSnapshots, &MsgSnapshot{src, node.id, messages[i]})
		}
	}
}

// Start a new snapshot at the node.
func (node *Node) StartSnapshot(snapshotId int) {

	// Initialize buffers for all inbound links
	inboundBuffers := make(map[string][]Message)
	for _, link := range node.inboundLinks {
		inboundBuffers[link.src] = make([]Message, 0)
	}

	if node.activeSnapshots[snapshotId] == nil {
		node.CreateLocalSnapshot(snapshotId, "")
	}

	// Send marker messages on all outbound links
	node.SendToNeighbors(Message{isMarker: true, data: snapshotId})
}
