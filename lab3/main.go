// Package main implements a complete Chord Distributed Hash Table (DHT) system.
// This implementation follows the Chord protocol design for distributed key-value storage
// and lookup in a peer-to-peer network environment.
package main

import (
	"bufio"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// BITS_SIZE_RING defines the size of the identifier space in the Chord ring.
// A 160-bit ring size allows for 2^160 possible node identifiers, matching SHA-1 hash size.
// This ensures a large enough address space to minimize the probability of ID collisions.
const BITS_SIZE_RING = 160

// Constants for encryption operations
const (
	// Key size for AES-256
	AES_KEY_SIZE = 32
	// Path for storing the encryption key
	ENCRYPTION_KEY_PATH = "./keys/aes.key"
	// File permissions for key storage
	KEY_PERMISSIONS = 0600
)

// File system related constants
const (
	// Base directory for storing DHT resources
	FOLDER_RESOURCES = "./resources"
	// File permissions: read/write for owner only
	PERMISSIONS_FILE = 0o600
	// Directory permissions: read/write/execute for owner only
	PERMISSIONS_DIR = 0o700
)

// RPC types for node communication

// SuccessorRep represents a response containing successor node information
type SuccessorRep struct {
	Successors []NodeInfo
}

// PredecessorRep represents a response containing predecessor node information
type PredecessorRep struct {
	Predecessor *NodeInfo
}

// StoreFileArgs represents arguments for file storage operations
type StoreFileArgs struct {
	Key     big.Int // File identifier in the DHT
	Content []byte  // Actual file content
}

// TransferFileArgs represents arguments for file transfer operations
type TransferFileArgs struct {
	Files map[string]*[]byte // Map of filenames to their contents
}

// FindSuccessorRep represents a response for successor lookup operations
type FindSuccessorRep struct {
	Found bool     // Whether the successor was found
	Node  NodeInfo // Information about the found node
}

// NodeAddress is a custom type for representing node network addresses
type NodeAddress string

// Key is a custom type for DHT keys
type Key string

// NodeInfo contains essential information about a node in the Chord ring
type NodeInfo struct {
	IpAddress  string  // Node's IP address
	ChordPort  int     // Port used for Chord protocol communication
	Identifier big.Int // Node's position in the Chord ring
}

// Node represents a complete node in the Chord DHT
type Node struct {
	Info            NodeInfo   // Basic node information
	FingerTable     []NodeInfo // Routing table for efficient lookups
	FingerTableSize int        // Maximum size of finger table
	Predecessor     *NodeInfo  // Previous node in the ring
	Successors      []NodeInfo // List of successor nodes for redundancy
	SuccessorsSize  int        // Number of successors to maintain
	NextFinger      int        // Next finger table entry to update
}

// main initializes and starts a Chord node with the specified configuration
// main initializes and starts the Chord node
func main() {
	// Parse command line flags
	var f Flags
	var errFlags = InitFlags(&f)
	if errFlags != nil {
		fmt.Printf("Flag reading error: %v\n", errFlags)
		return
	}

	// Generate or use provided node identifier
	var nodeIdentifier *big.Int
	if f.identifierOverride != INVALID_STRING {
		var res, errOptionalIdentifier = ConvertHexString(f.identifierOverride)
		if errOptionalIdentifier != nil {
			fmt.Printf("Identifier conversion error: %v\n", errOptionalIdentifier)
			return
		}
		nodeIdentifier = res
	} else {
		// Generate identifier from address and port
		address := NodeAddress(f.IpAddress) + ":" + NodeAddress(fmt.Sprintf("%v", f.ChordPort))
		nodeIdentifier = Hash(string(address))
	}

	// Initialize node-specific key management
	km := NewKeyManager(nodeIdentifier.String())
	if err := km.GenerateKey(); err != nil {
		fmt.Printf("Encryption initialization error: %v\n", err)
		return
	}

	// Initialize Chord ring configuration
	var NewRingcreate = f.CreateNewRingSet()
	var errChord = Start(f.IpAddress, f.ChordPort, BITS_SIZE_RING, f.SuccessorLimit,
		NewRingcreate, &f.JoinIpAddress, &f.JoinPort, nodeIdentifier)
	if errChord != nil {
		fmt.Printf("Chord Node initialization error: %v\n", errChord)
		return
	}

	// Initialize node's file system
	InitNodeFileSystem(nodeIdentifier.String())

	// Initialize RPC server
	var listener, errListener = net.Listen("tcp", ":"+fmt.Sprintf("%v", f.ChordPort))
	if errListener != nil {
		fmt.Printf("Listening Socket Initialization error: %v\n", errListener)
		return
	}
	RpcServerInit(&listener)

	// Schedule periodic maintenance tasks
	Schedule(Stabilize, time.Duration(f.TimeStabilize*int(time.Millisecond)))
	Schedule(FixFingers, time.Duration(f.TimeFixFingers*int(time.Millisecond)))
	Schedule(CheckPredecessor, time.Duration(f.TimeCheckPredecessor*int(time.Millisecond)))
	Schedule(FileBackup, time.Duration(f.TimeBackup*int(time.Millisecond)))

	// Start command line interface
	CommandsExecution()
}

// Flags struct holds all command-line configuration parameters
type Flags struct {
	IpAddress            string // Node's IP address
	ChordPort            int    // Port for Chord protocol
	JoinIpAddress        string // IP of existing node to join
	JoinPort             int    // Port of existing node to join
	TimeStabilize        int    // Interval between stabilize calls
	TimeFixFingers       int    // Interval between fix_fingers calls
	TimeCheckPredecessor int    // Interval between predecessor checks
	TimeBackup           int    // Interval between backups
	SuccessorLimit       int    // Maximum number of successors
	identifierOverride   string // Optional manual node identifier
}

// Constants for invalid flag values
const (
	INVALID_STRING = "NOT_VALID"
	INVALID_INT    = -1
)

// InitFlags initializes and validates command line flags
func InitFlags(f *Flags) error {
	flag.StringVar(&f.IpAddress, "a", INVALID_STRING, "The IP address that the Chord client will bind to, as well as advertise to other nodes. (e.g., 128.8.126.63)")
	flag.IntVar(&f.ChordPort, "p", INVALID_INT, "The port that the Chord client will bind to and listen on.")
	flag.StringVar(&f.JoinIpAddress, "ja", INVALID_STRING, "The IP address of the machine running a Chord node to join.")
	flag.IntVar(&f.JoinPort, "jp", INVALID_INT, "The port that an existing Chord node is listening on.")
	flag.IntVar(&f.TimeStabilize, "ts", INVALID_INT, "Time in milliseconds between stabilize calls [1,60000].")
	flag.IntVar(&f.TimeFixFingers, "tff", INVALID_INT, "Time in milliseconds between fix_fingers calls [1,60000].")
	flag.IntVar(&f.TimeCheckPredecessor, "tcp", INVALID_INT, "Time in milliseconds between check predecessor calls.")
	flag.IntVar(&f.TimeBackup, "tb", INVALID_INT, "Time in milliseconds between backup operations.")
	flag.IntVar(&f.SuccessorLimit, "r", INVALID_INT, "Number of successors maintained [1,32].")
	flag.StringVar(&f.identifierOverride, "i", INVALID_STRING, "Optional identifier override (40 character hex string).")
	flag.Parse()
	return FlagsValidation(f)
}

// RangeFlagValidation checks if a flag value is within specified range
func RangeFlagValidation(f, startRange, endRange int) bool {
	return startRange <= f && f <= endRange
}

// ErrorMessageCreation formats error messages for flag validation
func ErrorMessageCreation(flagname, description string) string {
	return fmt.Sprintf("Set %v: %v\n", flagname, description)
}

// FlagsValidation performs comprehensive validation of all command line flags
func FlagsValidation(f *Flags) error {
	var errorMessages []string

	// Validate IP Address
	if f.IpAddress == INVALID_STRING {
		errorMessages = append(errorMessages,
			ErrorMessageCreation("-a", "IP Address must be specified"))
	}

	// Validate Chord Port
	if f.ChordPort == INVALID_INT {
		errorMessages = append(errorMessages,
			ErrorMessageCreation("-p", "Port number must be specified"))
	} else if !RangeFlagValidation(f.ChordPort, 1024, 65535) {
		errorMessages = append(errorMessages,
			ErrorMessageCreation("-p", "Port must be between 1024 and 65535"))
	}

	// Validate Join Address and Port
	if (f.JoinIpAddress == INVALID_STRING && f.JoinPort != INVALID_INT) ||
		(f.JoinIpAddress != INVALID_STRING && f.JoinPort == INVALID_INT) {
		var flagname string
		if f.JoinIpAddress == INVALID_STRING {
			flagname = "-ja"
		} else {
			flagname = "-jp"
		}
		errorMessages = append(errorMessages,
			ErrorMessageCreation(flagname, "Both -ja and -jp must be specified together"))
	}

	// Validate Time Intervals
	if !RangeFlagValidation(f.TimeStabilize, 1, 60000) {
		errorMessages = append(errorMessages,
			ErrorMessageCreation("-ts", "Stabilize interval must be between 1 and 60000 milliseconds"))
	}

	if !RangeFlagValidation(f.TimeFixFingers, 1, 60000) {
		errorMessages = append(errorMessages,
			ErrorMessageCreation("-tff", "Fix fingers interval must be between 1 and 60000 milliseconds"))
	}

	if !RangeFlagValidation(f.TimeCheckPredecessor, 1, 60000) {
		errorMessages = append(errorMessages,
			ErrorMessageCreation("-tcp", "Check predecessor interval must be between 1 and 60000 milliseconds"))
	}

	if !RangeFlagValidation(f.TimeBackup, 1, 60000) {
		errorMessages = append(errorMessages,
			ErrorMessageCreation("-tb", "Backup interval must be between 1 and 60000 milliseconds"))
	}

	// Validate Successor Limit
	if !RangeFlagValidation(f.SuccessorLimit, 1, 32) {
		errorMessages = append(errorMessages,
			ErrorMessageCreation("-r", "Number of successors must be between 1 and 32"))
	}

	// Validate Optional Identifier Override
	if f.identifierOverride != INVALID_STRING {
		expectedLength := BITS_SIZE_RING / 4 // Convert bits to hex characters
		if _, err := hex.DecodeString(f.identifierOverride); err != nil ||
			len(f.identifierOverride) != expectedLength {
			errorMessages = append(errorMessages,
				ErrorMessageCreation("-i", fmt.Sprintf(
					"Identifier must be a valid hex string of exactly %d characters",
					expectedLength)))
		}
	}

	// If any validation errors occurred, combine them and return
	if len(errorMessages) > 0 {
		return fmt.Errorf("Flag validation failed:\n%s",
			strings.Join(errorMessages, ""))
	}

	return nil
}

// GetAdditionaIIdentifier returns the optional identifier override if specified
func (flag Flags) GetAdditionaIIdentifier() *string {
	if flag.identifierOverride == INVALID_STRING {
		return nil
	}
	return &flag.identifierOverride
}

// CreateNewRingSet determines if a new Chord ring should be created
func (flag Flags) CreateNewRingSet() bool {
	return flag.JoinIpAddress == INVALID_STRING && flag.JoinPort == INVALID_INT
}

// Lookup finds the node responsible for a given key in the DHT
// Now with improved failure handling
func Lookup(fileKey big.Int) (*NodeInfo, error) {
	var n = Get()
	var node, errFind = find(fileKey, n.Info, 32)
	if errFind != nil {
		// If primary node not found, try successors
		for _, successor := range n.Successors {
			// Try to get successor's successors list
			succList, errSucc := Successors(ObtainChordAddress(successor))
			if errSucc != nil {
				continue
			}

			// Try each successor in the list
			for _, s := range succList {
				// Check if node is alive
				if !CheckAlive(ObtainChordAddress(s)) {
					continue
				}

				// Get predecessor to check responsibility
				pred, errPred := Predecessor(ObtainChordAddress(s))
				if errPred != nil {
					continue
				}

				// If no predecessor or we can determine responsibility
				if pred == nil {
					// If this is the only node, it's responsible
					if len(succList) == 1 {
						return &s, nil
					}
					continue
				}

				// Check if this node is responsible for the file
				if Between(&pred.Identifier, &fileKey, &s.Identifier, true) {
					return &s, nil
				}
			}
		}
		return nil, fmt.Errorf("no responsible node found: %w", errFind)
	}
	return node, nil
}

// FileMetadata contains information about the stored file
type FileMetadata struct {
	UploadNodeID string    // ID of the node that uploaded the file
	Encrypted    bool      // Whether the file is encrypted
	Timestamp    time.Time // When the file was stored
}

// FileContent combines file metadata with its content
type FileContent struct {
	Metadata FileMetadata // File metadata
	Content  []byte       // File content (may be encrypted)
}

// StoreFile handles the storage of a file in the DHT with optional encryption
// Returns the responsible node, file key, and any error encountered
func StoreFile(filePath string, encrypted bool) (*NodeInfo, *big.Int, error) {
	// Validate the input file path
	if err := validateFilePath(filePath); err != nil {
		return nil, nil, fmt.Errorf("invalid file path: %w", err)
	}

	// Get current node information
	currentNode := Get()
	nodeID := currentNode.Info.Identifier.String()

	// Calculate file identifier
	fileName := GetFileName(filePath)
	fileKey := Hash(fileName)

	// Find the responsible node in the DHT
	node, errLookup := Lookup(*fileKey)
	if errLookup != nil {
		return nil, nil, fmt.Errorf("lookup failed: %w", errLookup)
	}

	// Read the file content
	content, errRead := ReadFile(filePath)
	if errRead != nil {
		return nil, nil, fmt.Errorf("failed to read file: %w", errRead)
	}

	// Create file metadata
	metadata := FileMetadata{
		UploadNodeID: nodeID,
		Encrypted:    encrypted,
		Timestamp:    time.Now(),
	}

	// Handle encryption if requested
	if encrypted {
		// Initialize key management for the current node
		km := NewKeyManager(nodeID)
		if err := km.GenerateKey(); err != nil {
			return nil, nil, fmt.Errorf("failed to initialize encryption: %w", err)
		}

		// Create encryptor and encrypt content
		fe := NewFileEncryptor(km)
		encryptedContent, err := fe.EncryptFile(content)
		if err != nil {
			return nil, nil, fmt.Errorf("encryption failed: %w", err)
		}
		content = encryptedContent
	}

	// Prepare the complete file content structure
	fileContent := FileContent{
		Metadata: metadata,
		Content:  content,
	}

	// Serialize the content structure
	serializedContent, err := json.Marshal(fileContent)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to serialize file content: %w", err)
	}

	// Store the file either locally or remotely
	if node.Identifier.Cmp(&currentNode.Info.Identifier) == 0 {
		// Local storage
		err := NodeFileWrite(fileKey.String(), currentNode.Info.Identifier.String(), serializedContent)
		if err != nil {
			return nil, nil, fmt.Errorf("local file write failed: %w", err)
		}
	} else {
		// Remote storage via RPC
		err := StoreFileClient(ObtainChordAddress(*node), *fileKey, serializedContent)
		if err != nil {
			return nil, nil, fmt.Errorf("RPC storage failed: %w", err)
		}
	}

	fmt.Printf("File stored successfully at node %v with key %v\n",
		node.Identifier.String(), fileKey.String())

	return node, fileKey, nil
}

// KeyManager and FileEncryptor types and methods as defined in previous response...

// Helper function to validate file path
func validateFilePath(path string) error {
	// Check if file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return fmt.Errorf("file does not exist: %s", path)
	}

	// Check if file is readable
	file, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("cannot open file: %w", err)
	}
	file.Close()

	return nil
}

// ObtainNodeState generates a string representation of a node's state
// Includes node identifier, IP address, port numbers, and optional index information
func ObtainNodeState(node NodeInfo, collectionItem bool, index int, idealIdentifier *big.Int) (*string, error) {
	var nodeInfo = fmt.Sprintf("\nNode Identifier: %v\nNode IP address: %v | Chord Node Port Number: %v ", node.Identifier.String(), node.IpAddress, node.ChordPort)
	if collectionItem {
		nodeInfo += fmt.Sprintf("\nNode Index Value: %v | Node Index Identifier: %v", index, idealIdentifier)
	}
	nodeInfo += "\n"
	return &nodeInfo, nil
}

// CalculatePossibleIdentifier is a function type for calculating node identifiers
// Used for both finger table and successor list calculations
type CalculatePossibleIdentifier func(int) big.Int

// ObtainNodeArrayState generates a string representation of an array of nodes
// Uses the provided calculator function to determine ideal identifiers
func ObtainNodeArrayState(nodes []NodeInfo, calculatePossibleIdentifier CalculatePossibleIdentifier) (*string, error) {
	var state = new(string)
	for index, item := range nodes {
		var idealIdentifier = calculatePossibleIdentifier(index)
		var info, err = ObtainNodeState(item, true, index, &idealIdentifier)
		if err != nil {
			return nil, err
		}
		*state += *info + "\n\n"
	}
	return state, nil
}

// ObtainState generates a complete state report for the current node
// Includes predecessor, successors, and finger table information
func ObtainState() (*string, error) {
	var n = Get()
	var state, err = ObtainNodeState(n.Info, false, -1, nil)
	if err != nil {
		return nil, err
	}
	*state += "NodePredecessor: "
	if n.Predecessor == nil {
		*state += "None \n"
	} else {
		*state += n.Predecessor.Identifier.String()
	}

	*state += "\n\nNodeSuccessors:\n"

	var successorStatus, successorErr = ObtainNodeArrayState(n.Successors, func(i int) big.Int {
		return *new(big.Int).Add(big.NewInt(int64(i+1)), &n.Info.Identifier)
	})
	if successorErr != nil {
		return nil, successorErr
	}
	if successorStatus == nil {
		*state += "No successors available\n"
	} else {
		*state += *successorStatus
	}

	*state += "\nNodeFingertable:\n"

	var fingerTableStatus, fingerTableErr = ObtainNodeArrayState(n.FingerTable, func(i int) big.Int {
		return *Jump(n.Info.Identifier, i)
	})
	if fingerTableErr != nil {
		return nil, fingerTableErr
	}
	if fingerTableStatus == nil {
		*state += "No finger table entries available\n"
	} else {
		*state += *fingerTableStatus
	}
	return state, nil
}

// FindSuccessor implements the core Chord lookup operation
// Returns true and the successor if found, false and closest preceding node otherwise
func FindSuccessor(id big.Int) (bool, NodeInfo) {
	var n = Get()
	// Check if id falls between current node and its immediate successor
	if Between(&n.Info.Identifier, &id, &n.Successors[0].Identifier, true) {
		// fmt.Printf("[findSuccessor] Nodeid: %v, return value: %v, %v", id, true, n.Successors[0])
		return true, n.Successors[0]
	}
	// Otherwise, return the closest preceding node
	var NearestNode = NearestPrecedingNode(id)
	// fmt.Printf("[findSuccessor] Nodeid: %v, return value: %v, %v", id, false, NearestNode)
	return false, NearestNode
}

// find iteratively looks for the successor of an identifier
// Implements the Chord protocol's find_successor operation with a maximum step limit
func find(id big.Int, start NodeInfo, maxSteps int) (*NodeInfo, error) {
	var found = false
	var nextNode = start
	for i := 0; i < maxSteps; i++ {
		var res, err = ClientLocateSuccessor(ObtainChordAddress(nextNode), &id)
		if err != nil {
			return nil, err
		}
		found = res.Found
		if found {
			return &res.Node, nil
		}
		nextNode = res.Node
	}
	return nil, errors.New("Successors Could not be found")
}

// locateNearestPrecedingCandidate finds the closest preceding node from a given table
// Used by both finger table and successor list lookups
func locateNearestPrecedingCandidate(n Node, table []NodeInfo, id big.Int) *NodeInfo {
	for i := len(table) - 1; i >= 0; i-- {
		if Between(&n.Info.Identifier, &table[i].Identifier, &id, false) {
			return &table[i]
		}
	}
	return nil
}

// NearestPrecedingNode finds the closest preceding node for a given identifier
// Implements the Chord protocol's closest_preceding_node operation
func NearestPrecedingNode(id big.Int) NodeInfo {
	var n = Get()
	// Check finger table first
	var candidate *NodeInfo = locateNearestPrecedingCandidate(n, n.FingerTable, id)
	// Then check successor list
	if c := locateNearestPrecedingCandidate(n, n.Successors, id); candidate == nil || (c != nil &&
		Between(&id, &c.Identifier, &candidate.Identifier, false)) {
		candidate = c
	}
	if candidate != nil {
		// fmt.Printf("[NearestPrecedingNode] Nodeid: %v, return value: %v\n", id, *candidate)
		return *candidate
	}
	// fmt.Printf("[NearestPrecedingNode] Nodeid: %v, return value: %v\n", id, n.Info)
	return n.Info
}

// Start initializes a new Chord node and either creates a new ring or joins an existing one
// This is the main entry point for node initialization
func Start(ipAddress string, chordPort, fingerTableSize, successorsSize int, NewRingcreate bool,
	joinIpAddress *string, joinPort *int, additionalIdetifier *big.Int) error {
	fmt.Printf("Starting Chord Node at %v:%v\n", ipAddress, chordPort)
	// Initialize node instance
	var err = NodeInstanceInit(ipAddress, chordPort, fingerTableSize, successorsSize, additionalIdetifier)
	if err != nil {
		return err
	}
	if NewRingcreate {
		create()
		return nil
	}
	if joinIpAddress == nil || joinPort == nil {
		return errors.New("Joining ip address and port number required when NewRingcreate is set to false")
	}
	var temp = Get().Info.Identifier
	return join(*joinIpAddress, *joinPort, &temp, fingerTableSize)
}

// create initializes a new Chord ring with this node as the only member
func create() {
	NewSuccessor(Get().Info)
	NewFingerTable(Get().Info)
}

// join makes this node join an existing Chord ring
// Implements the Chord protocol's join operation
func join(joinIpAddress string, joinPort int, nodeId *big.Int, maxSteps int) error {
	var joinHash = big.NewInt(-1)

	var successor, errFind = find(*nodeId, NodeInfo{
		IpAddress:  joinIpAddress,
		ChordPort:  joinPort,
		Identifier: *joinHash}, maxSteps)
	if errFind != nil {
		return errFind
	}
	NewSuccessor(*successor)
	NewFingerTable(*successor)
	return nil
}

// Leave handles node departure from the ring (currently unimplemented)
func Leave(n *Node) {
}

// Stabilize implements the Chord protocol's stabilize operation
// Periodically verifies node's immediate successor and updates successor list
// This operation maintains the correct successor pointer and successor list
// for each node in the Chord ring
func Stabilize() {
	var x *NodeInfo
	var n = Get()

	// If successor list is empty, point to self as the only known node
	if len(n.Successors) == 0 {
		NewSuccessor(n.Info)
		return
	}

	// Try to get predecessor from successors with retries
	var allSuccessorsDown = true
	maxRetries := 3
	var currentSuccessor = n.Successors[0] // Keep track of current immediate successor

	// Iterate through successors to find a responsive one and get its predecessor
	for index, successor := range n.Successors {
		var err error
		// Implement retry mechanism for network reliability
		for retry := 0; retry < maxRetries; retry++ {
			x, err = Predecessor(ObtainChordAddress(successor))
			if err == nil {
				allSuccessorsDown = false
				// Update current successor if we had to fall back to a backup successor
				if index > 0 {
					currentSuccessor = successor
				}
				break
			}
			if retry < maxRetries-1 {
				// Wait before retry to avoid overwhelming the network
				time.Sleep(time.Millisecond * 100)
			} else {
				fmt.Printf("[Stabilize] Failed to contact successor %v after %d retries: %v\n",
					successor.Identifier.String(), maxRetries, err)
			}
		}
		if !allSuccessorsDown {
			break
		}
	}

	// If all known successors are unresponsive, point to self
	if allSuccessorsDown {
		fmt.Printf("[Stabilize] All successors appear to be down, falling back to self\n")
		NewSuccessor(n.Info)
		return
	}

	// Update successor pointers if necessary
	// Case 1: Found a new candidate for immediate successor (x)
	// Case 2: Current successor's predecessor (x) is better than current successor
	if x != nil {
		// Check if x should be our immediate successor
		if Between(&n.Info.Identifier, &x.Identifier, &currentSuccessor.Identifier, false) {
			fmt.Printf("[Stabilize] Found better successor: %v\n", x.Identifier.String())
			NewSuccessor(*x)
			currentSuccessor = *x
		}
	}

	// Notify successor and update successor list
	if NoOfSuccessors() > 0 {
		var notifySuccess = false
		// Attempt to notify successor with retries
		for retry := 0; retry < maxRetries; retry++ {
			errNotify := ClientNotification(ObtainChordAddress(currentSuccessor), n.Info)
			if errNotify == nil {
				notifySuccess = true
				break
			}
			if retry < maxRetries-1 {
				time.Sleep(time.Millisecond * 100)
			} else {
				fmt.Printf("[Stabilize] Failed to notify successor %v after %d retries: %v\n",
					currentSuccessor.Identifier.String(), maxRetries, errNotify)
			}
		}

		// If notification was successful, try to update successor list
		if notifySuccess {
			var successors []NodeInfo
			var fetchSuccess = false

			// Attempt to get successor's successor list with retries
			for retry := 0; retry < maxRetries; retry++ {
				var errSuc error
				successors, errSuc = Successors(ObtainChordAddress(currentSuccessor))
				if errSuc == nil {
					fetchSuccess = true
					break
				}
				if retry < maxRetries-1 {
					time.Sleep(time.Millisecond * 100)
				} else {
					fmt.Printf("[Stabilize] Failed to fetch successor list from %v after %d retries: %v\n",
						currentSuccessor.Identifier.String(), maxRetries, errSuc)
				}
			}

			// Update local successor list if we successfully got successor's list
			if fetchSuccess && len(successors) > 0 {
				// Create a new list starting with our immediate successor
				var newSuccessors = []NodeInfo{currentSuccessor}

				// Add successors from our successor's list
				for _, s := range successors {
					// Don't add ourselves to the list
					if s.Identifier.Cmp(&n.Info.Identifier) != 0 {
						newSuccessors = append(newSuccessors, s)
					}
				}

				// Trim the list to maximum size while ensuring at least one successor
				if len(newSuccessors) > n.SuccessorsSize {
					newSuccessors = newSuccessors[:n.SuccessorsSize]
				}

				// Update the successor list
				instance.Successors = newSuccessors

				//fmt.Printf("[Stabilize] Updated successor list, length: %d\n", len(instance.Successors))
			}
		}
	}
}

// Notify implements the Chord protocol's notify operation
// Called when a node thinks it might be our predecessor
func Notify(node NodeInfo) {
	var n = Get()

	// Update predecessor if current node has no predecessor,
	// or if the new node is between current predecessor and current node
	if n.Predecessor == nil || n.Predecessor.Identifier.Cmp(&n.Info.Identifier) == 0 ||
		Between(&n.Predecessor.Identifier, &node.Identifier, &n.Info.Identifier, false) {
		DefinePredecessor(&node)
	}
}

// FixFingers implements the Chord protocol's fix_fingers operation
// Periodically refreshes entries in the finger table
func FixFingers() {
	var n = Get()
	next := NextFingerUpdation()

	// Calculate the identifier for this finger table entry
	identifierToFix := *Jump(n.Info.Identifier, next)

	// Maximum number of retries
	maxRetries := 3
	var node *NodeInfo
	var err error

	// Try to find the responsible node with retries
	for retry := 0; retry < maxRetries; retry++ {
		node, err = find(identifierToFix, n.Info, 32)
		if err == nil {
			break
		}
		if retry < maxRetries-1 {
			time.Sleep(time.Millisecond * 100)
		}
	}

	if err != nil {
		fmt.Printf("[FixFingers] Failed to find node for finger[%d] = %v after %d retries: %v\n",
			next, identifierToFix.String(), maxRetries, err)
		return
	}

	// Update the finger table entry
	if err := DefineFingerTableIndex(next, *node); err != nil {
		//fmt.Printf("[FixFingers] Failed to update finger[%d]: %v\n", next, err)
	} else {
		//fmt.Printf("[FixFingers] Updated finger[%d] = %v\n", next, node.Identifier.String())
	}
}

// CheckPredecessor implements the Chord protocol's check_predecessor operation
// Periodically verifies if predecessor is still alive
func CheckPredecessor() {
	var n = Get()
	if n.Predecessor != nil && !CheckAlive(ObtainChordAddress(*n.Predecessor)) {
		DefinePredecessor(nil)
		// fmt.Printf("[checkPredecessor] Node Predecessor set to nil as previous Node Predecessorr %v was not responsive\n", n.Predecessor)

	}
}

// ObtainMissingFiles fetches list of missing files from each successor
// Used in file backup process
func ObtainMissingFiles(successors []NodeInfo, files []string) [][]string {
	var unavailableFiles [][]string
	var wg = new(sync.WaitGroup)
	wg.Add(len(successors))

	// Concurrently check each successor for missing files
	for _, successor := range successors {
		go func(address NodeAddress) {
			var rep, err = MissingFiles(address, files)
			if err != nil {
				unavailableFiles = append(unavailableFiles, []string{})
			} else {
				unavailableFiles = append(unavailableFiles, rep)
			}
			wg.Done()
		}(ObtainChordAddress(successor))
	}
	wg.Wait()
	return unavailableFiles
}

// filterAvailableFiles filters files that should be stored on this node
// based on the Chord identifier space
func filterAvailableFiles(files []string, predecessorIdentifier big.Int, nodeIdentifier big.Int) []string {
	var f = []string{}
	for _, item := range files {
		var temp = new(big.Int)
		var fileIdentifier, ok = temp.SetString(item, 10)
		// fmt.Printf("[chordFunctions.filterAvailableFiles] converting %v to %v with confirmation as: %v\n", item, fileIdentifier, ok)
		if ok && !Between(fileIdentifier, &predecessorIdentifier, &nodeIdentifier, false) {
			f = append(f, item)
		}
	}
	return f
}

// FileBackup implements periodic file backup to successor nodes
// Ensures data redundancy in the DHT
func FileBackup() {
	var n = Get()
	if n.Predecessor == nil {
		return
	}
	// fmt.Printf("[chordFunctions.FileBackup] Requested")
	// Acquire lock for file operations
	ObtainRWLock(false)
	// fmt.Printf("[chordFunctions.FileBackup] ReadWrite lock obtained ")
	var successors = n.Successors
	var nodeKey = n.Info.Identifier.String()
	var f, err = ListFiles(nodeKey)

	// Filter files that should be backed up
	var ownedFiles = filterAvailableFiles(f, n.Predecessor.Identifier, n.Info.Identifier)
	if err != nil || len(ownedFiles) <= 0 || len(n.Successors) <= 0 {
		LiberateRWLock(false)
		return
	}
	//fmt.Printf("[chordFunctions.FileBackup] Available Files in the system: %v\n", ownedFiles)

	// Check which successors are missing which files
	var unavailableFiles = ObtainMissingFiles(successors, ownedFiles)
	var fls = NodeFilesRead(nodeKey, ownedFiles)
	//fmt.Printf("[chordFunctions.FileBackup] Files Missing for Successors: %v\n", unavailableFiles)

	// Concurrently transfer missing files to successors
	var wg = new(sync.WaitGroup)
	wg.Add(len(successors))
	for index, item := range successors {
		go func(i int, node NodeInfo) {
			var filesToTransfer = map[string]*[]byte{}
			if i < len(unavailableFiles) {
				for _, missingFile := range unavailableFiles[i] {
					var file, ok = fls[missingFile]
					if ok {
						filesToTransfer[missingFile] = file
					}
				}
				if len(filesToTransfer) > 0 {
					FileTransfer(ObtainChordAddress(node), filesToTransfer)
				}
			}
			wg.Done()
		}(index, item)
	}
	wg.Wait()

	LiberateRWLock(false)
}

// Singleton instance for the Chord node
var once sync.Once
var instance Node

// NodeInstanceInit initializes the Chord node as a singleton
// Sets up the node's initial state including finger table and successor list
func NodeInstanceInit(ipAddress string, chordPort, fingerTableSize, successorsSize int, additionalIdetifier *big.Int) error {
	if fingerTableSize < 1 || successorsSize < 1 {
		return errors.New("The Size needs to be at least 1")
	}

	once.Do(func() {
		var address = NodeAddress(ipAddress) + ":" + NodeAddress(fmt.Sprintf("%v", chordPort))
		var info NodeInfo
		if additionalIdetifier == nil {
			info = NodeInfo{
				IpAddress:  ipAddress,
				ChordPort:  chordPort,
				Identifier: *Hash(string(address)),
			}
		} else {
			info = NodeInfo{
				IpAddress:  ipAddress,
				ChordPort:  chordPort,
				Identifier: *additionalIdetifier,
			}
		}

		instance = Node{
			Info:            info,
			FingerTable:     []NodeInfo{},
			Predecessor:     nil,
			FingerTableSize: fingerTableSize,
			Successors:      []NodeInfo{},
			SuccessorsSize:  successorsSize,
			NextFinger:      -1,
		}
	})
	return nil
}

// ObtainChordAddress formats a node's address for Chord protocol communication
func ObtainChordAddress(node NodeInfo) NodeAddress {
	return NodeAddress(fmt.Sprintf("%v:%v", node.IpAddress, node.ChordPort))
}

// NextFingerUpdation returns the next finger table entry to update
// Implements round-robin updating of finger table entries
func NextFingerUpdation() int {
	instance.NextFinger = (instance.NextFinger + 1) % instance.FingerTableSize
	return instance.NextFinger
}

// NewSuccessor updates the node's successor list
func NewSuccessor(successor NodeInfo) {
	// 不要清空现有的后继列表，而是更新第一个后继
	if len(instance.Successors) == 0 {
		instance.Successors = []NodeInfo{successor}
	} else {
		// 如果successor已经在列表中且不是第一个，需要调整顺序
		found := false
		for i, s := range instance.Successors {
			if s.Identifier.Cmp(&successor.Identifier) == 0 {
				if i != 0 { // 如果不是第一个位置，需要移动到第一个位置
					// 删除当前位置
					instance.Successors = append(instance.Successors[:i], instance.Successors[i+1:]...)
					// 插入到第一个位置
					instance.Successors = append([]NodeInfo{successor}, instance.Successors...)
				}
				found = true
				break
			}
		}
		if !found {
			// 如果是新的successor，插入到第一个位置
			instance.Successors = append([]NodeInfo{successor}, instance.Successors...)
		}
		// 确保列表不超过最大长度
		if len(instance.Successors) > instance.SuccessorsSize {
			instance.Successors = instance.Successors[:instance.SuccessorsSize]
		}
	}
}

// NewFingerTable initializes the finger table with proper size
func NewFingerTable(successor NodeInfo) {
	// Initialize finger table with successor for all entries
	instance.FingerTable = make([]NodeInfo, instance.FingerTableSize)
	for i := 0; i < instance.FingerTableSize; i++ {
		instance.FingerTable[i] = successor
	}
}

// SuccesorsContainsItself checks if the node itself appears in the successor list
// Returns the index if found, -1 otherwise
func SuccesorsContainsItself(successors []NodeInfo) int {
	for index, item := range successors {
		if item.Identifier.Cmp(&instance.Info.Identifier) == 0 {
			return index
		}
	}
	return -1
}

// AddSuccessors updates the successor list while maintaining size limits
func AddSuccessors(successors []NodeInfo) {
	if len(successors) == 0 {
		return
	}

	// 移除自己（如果存在）
	var filteredSuccessors []NodeInfo
	for _, s := range successors {
		if s.Identifier.Cmp(&instance.Info.Identifier) != 0 {
			filteredSuccessors = append(filteredSuccessors, s)
		}
	}

	// 合并当前successors和新的successors
	var mergedList []NodeInfo
	existingMap := make(map[string]bool)

	// 首先添加当前的第一个successor（如果有的话）
	if len(instance.Successors) > 0 {
		mergedList = append(mergedList, instance.Successors[0])
		existingMap[instance.Successors[0].Identifier.String()] = true
	}

	// 添加新的successors
	for _, s := range filteredSuccessors {
		if !existingMap[s.Identifier.String()] {
			mergedList = append(mergedList, s)
			existingMap[s.Identifier.String()] = true
		}
	}

	// 添加剩余的当前successors
	for i := 1; i < len(instance.Successors); i++ {
		if !existingMap[instance.Successors[i].Identifier.String()] {
			mergedList = append(mergedList, instance.Successors[i])
			existingMap[instance.Successors[i].Identifier.String()] = true
		}
	}

	// 限制列表大小
	if len(mergedList) > instance.SuccessorsSize {
		mergedList = mergedList[:instance.SuccessorsSize]
	}
	// fmt.Printf("Present successor is %v, new successors being added: %v\n", instance.Successors[0], elementsAddition)
	instance.Successors = mergedList
}

// Successor returns the node's current successor
func Successor() NodeInfo {
	return instance.Successors[0]
}

// NoOfSuccessors returns the current number of successors
func NoOfSuccessors() int {
	return len(instance.Successors)
}

// DefineFingerTableIndex updates a specific finger table entry
func DefineFingerTableIndex(index int, item NodeInfo) error {
	if index >= instance.FingerTableSize {
		return fmt.Errorf("index %d exceeds finger table size %d", index, instance.FingerTableSize)
	}

	// Simply update the entry at the given index
	instance.FingerTable[index] = item
	return nil
}

// DefinePredecessor updates the node's predecessor
func DefinePredecessor(predecessor *NodeInfo) {
	instance.Predecessor = predecessor
}

// Get returns the current node instance
func Get() Node {
	return instance
}

//commands.go

// Command represents a CLI command with its parameter requirements
type Command struct {
	requiredParamers   int    // Number of required parameters
	optionalParameters int    // Number of optional parameters
	usageString        string // Usage instructions
}

// GetCommands returns the map of available CLI commands
func GetCommands() map[string]Command {
	return map[string]Command{
		"Lookup":     {1, 0, "usage: Lookup <filename>"},
		"l":          {1, 0, "usage: Lookup <filename>"},
		"StoreFile":  {1, 2, "usage: StoreFile <filepathOnDisk> [ssh: default=true, f or false to disable] encrypt file: default=true, f or false to disable]"},
		"s":          {1, 2, "usage: StoreFile <filepathOnDisk> [ssh: default=true, f or false to disable] [encrypt file: default=true, f or false to disable]"},
		"PrintState": {0, 0, "usage: PrintState"},
		"p":          {0, 0, "usage: PrintState"},
	}
}

// validateCommand checks if a command and its arguments are valid
func validateCommand(cmdArr []string) error {
	if len(cmdArr) <= 0 {
		return errors.New("Provide Command")
	}
	var cmd, ok = GetCommands()[cmdArr[0]]
	if !ok {
		return errors.New("Provided Command " + cmdArr[0] + " is not available")
	}
	if len(cmdArr)-1 < cmd.requiredParamers || len(cmdArr)-1 > cmd.optionalParameters+cmd.requiredParamers {
		return errors.New(cmd.usageString)
	}
	return nil
}

// ObtainTurnOffOption parses boolean command options
func ObtainTurnOffOption(cmdArr []string, index int) bool {
	if len(cmdArr) > index && (strings.ToLower(cmdArr[index]) == "false" || strings.ToLower(cmdArr[index]) == "f") {
		return false
	}
	return true
}

// CommandExecution handles execution of individual commands
func CommandExecution(cmdArr []string) {
	switch cmdArr[0] {
	case "Lookup", "l":
		var answer, errLookup = Lookup(*Hash(cmdArr[1]))
		var fileHash = Hash(cmdArr[1]) // Calculate file hash first
		if errLookup != nil {
			fmt.Println(errLookup.Error())
			return
		}
		var state, errState = ObtainNodeState(*answer, false, -1, nil)
		if errState != nil {
			fmt.Println(errState.Error())
			return
		}
		fmt.Println(*state)
		// Try to read and print file content
		var content, errRead = ReadNodeFile(answer.Identifier.String(), fileHash.String())
		if errRead != nil {
			fmt.Printf("fail to read content: %v\n", errRead)
			return
		}
		fmt.Printf("\ncontent of file:\n%s\n", content)

	case "StoreFile", "s":
		var encryption = ObtainTurnOffOption(cmdArr, 3)
		var node, fileKey, errStore = StoreFile(cmdArr[1], encryption)
		if errStore != nil {
			fmt.Println(errStore.Error())
			return
		}
		var state, errState = ObtainNodeState(*node, false, -1, nil)
		if errState != nil {
			fmt.Println(errState.Error())
			return
		}
		fmt.Printf("FileKey: %v\nStored at:\n%v\n", fileKey.String(), *state)

	case "PrintState", "p":
		var output, err = ObtainState()
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		fmt.Println(*output)

	default:
		fmt.Println("command is not available")
	}
}

// CommandsExecution provides the main command line interface loop
func CommandsExecution() {
	var scanner = bufio.NewReader(os.Stdin)
	for {
		fmt.Print("\nChord Node Prompt: ")
		var arguments, errRead = scanner.ReadString('\n')
		if errRead != nil {
			fmt.Println("Commands to be entered on a single line")
			continue
		}
		var cmdArr = strings.Fields(arguments)
		var errValidate = validateCommand(cmdArr)
		if errValidate != nil {
			fmt.Println(errValidate.Error())
			continue
		}
		CommandExecution(cmdArr)
	}
}

// Global RWLock for thread-safe file operations
var lock = new(sync.RWMutex)

// ObtainRWLock acquires the appropriate lock (read or write)
func ObtainRWLock(write bool) {
	if write {
		lock.Lock()
		return
	}
	lock.RLock()
}

// LiberateRWLock releases the appropriate lock
func LiberateRWLock(write bool) {
	if write {
		lock.Unlock()
		return
	}
	lock.RUnlock()
}

// InitNodeFileSystem initializes the file system directory for a node
func InitNodeFileSystem(nodeKey string) error {
	var folder = ObtainFileDir(nodeKey)
	var err = os.MkdirAll(folder, PERMISSIONS_DIR)
	if err != nil {
		return err
	}
	return nil
}

// ListFiles lists files only from node-specific directory
func ListFiles(nodeKey string) ([]string, error) {
	dir, err := os.ReadDir(ObtainFileDir(nodeKey))
	if err != nil {
		fmt.Printf("[files.ListFiles] could not read the directory: %v\n", err)
		return nil, err
	}

	var files = []string{}
	for _, file := range dir {
		files = append(files, file.Name())
	}
	return files, nil
}

// GetFileName extracts the filename from a full path
func GetFileName(filePath string) string {
	var paths = strings.Split(filePath, "/")
	return paths[len(paths)-1]
}

// ReadFile reads the content of a file from disk
func ReadFile(filePath string) ([]byte, error) {
	var file, errRead = os.ReadFile(filePath)
	return file, errRead
}

// ReadNodeFile reads and potentially decrypts a file from the DHT
// Now checks only node-specific directories
func ReadNodeFile(nodeKey, fileKey string) ([]byte, error) {
	var currentNode = Get()
	var currentNodeID = currentNode.Info.Identifier.String()

	// Helper function to read and parse file content
	readAndParseFile := func(path string) ([]byte, error) {
		content, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("failed to read file: %w", err)
		}

		// Try to parse as FileContent structure
		var fileContent FileContent
		if err := json.Unmarshal(content, &fileContent); err != nil {
			// If parsing fails, might be an old format file
			// Return content as-is for backward compatibility
			return content, nil
		}

		km := NewKeyManager(currentNodeID)
		fe := NewFileEncryptor(km)

		// try decryption.
		decryptedContent, err := fe.DecryptFile(fileContent.Content)
		if err != nil {
			return fileContent.Content, nil
		}
		return decryptedContent, nil
	}

	// Try reading from the specified node's directory
	nodePath := filepath.Join("resources", nodeKey, fileKey)
	if content, err := readAndParseFile(nodePath); err == nil {
		return content, nil
	}

	// If file not found and we're looking for a file that should be on this node,
	// try reading from successor nodes
	for _, successor := range currentNode.Successors {
		fmt.Printf("Attempting to read from successor node: %s\n",
			successor.Identifier.String())

		content, err := ReadRemoteFile(successor, fileKey)
		if err == nil {
			// Save a local copy and process it
			_ = os.MkdirAll(filepath.Join("resources", nodeKey), PERMISSIONS_DIR)
			_ = NodeFileWrite(fileKey, nodeKey, content)
			return readAndParseFile(filepath.Join("resources", nodeKey, fileKey))
		}
	}

	return nil, fmt.Errorf("file not found: %s", fileKey)
}

// ReadRemoteFile attempts to read a file from a remote node
func ReadRemoteFile(node NodeInfo, fileKey string) ([]byte, error) {
	// 构建远程文件的完整路径
	remoteFilePath := filepath.Join(FOLDER_RESOURCES, node.Identifier.String(), fileKey)
	fmt.Printf("Attempting to read remote file: %s\n", remoteFilePath)

	// 尝试通过 RPC 读取
	return ReadFileRPC(ObtainChordAddress(node), fileKey)
}

// ReadFileRPC reads a file from a remote node using RPC
func ReadFileRPC(nodeAddress NodeAddress, fileKey string) ([]byte, error) {
	var reply FileContentReply
	err := handleCall(nodeAddress, "ChordRPCHandler.ReadFile", &fileKey, &reply)
	if err != nil {
		return nil, err
	}
	return reply.Content, nil
}

// Add new RPC handler for file reading
type FileContentReply struct {
	Content []byte
}

// RPC handler for file reading
func (t *ChordRPCHandler) ReadFile(fileKey *string, reply *FileContentReply) error {
	if fileKey == nil {
		return fmt.Errorf("invalid file key")
	}

	n := Get()
	nodeKeyInt := &n.Info.Identifier
	nodeKey := nodeKeyInt.String()

	content, err := os.ReadFile(ObtainFilePath(*fileKey, nodeKey))
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	// 不在RPC层进行解密，保持原始加密状态传输
	reply.Content = content
	return nil
}

// NodeFilesRead reads multiple files from a node's storage
func NodeFilesRead(nodeKey string, filePaths []string) map[string]*[]byte {
	var files = make(map[string]*[]byte)
	for _, filePath := range filePaths {
		var content, err = ReadNodeFile(nodeKey, filePath)
		if err == nil {
			files[filePath] = &content
		}
	}
	return files
}

// ObtainFileDir returns the full path to a node's storage directory
func ObtainFileDir(nodeKey string) string {
	return filepath.Join(FOLDER_RESOURCES, nodeKey)
}

// ObtainFilePath now only returns path in node-specific directory
func ObtainFilePath(key, nodeKey string) string {
	return filepath.Join(ObtainFileDir(nodeKey), key)
}

// NodeFileWrite writes content to a file in a node's storage
func NodeFileWrite(key, nodeKey string, content []byte) error {
	// Create node-specific directory if it doesn't exist
	nodeDir := filepath.Join("resources", nodeKey)
	if err := os.MkdirAll(nodeDir, PERMISSIONS_DIR); err != nil {
		return fmt.Errorf("failed to create node directory: %w", err)
	}

	// Write file only to node-specific directory
	return os.WriteFile(filepath.Join(nodeDir, key), content, PERMISSIONS_FILE)
}

// NodeFilesWrite writes multiple files to a node's storage
func NodeFilesWrite(nodeKey string, files map[string]*[]byte) []error {
	var folder = ObtainFileDir(nodeKey)
	var err = os.MkdirAll(folder, PERMISSIONS_DIR)
	if err != nil {
		return []error{err}
	}
	var errsWrite = []error{}
	for key, content := range files {
		var errWrite = os.WriteFile(ObtainFilePath(key, nodeKey), *content, PERMISSIONS_FILE)
		if errWrite != nil {
			errsWrite = append(errsWrite, errWrite)
		}
	}
	return errsWrite
}

// AddToFile appends content to an existing file
func AddToFile(key, nodeKey string, content []byte) (int, error) {
	var file, errOpen = os.OpenFile(ObtainFilePath(key, nodeKey), os.O_APPEND|os.O_WRONLY, PERMISSIONS_FILE)
	if errOpen != nil {
		return 0, errOpen
	}
	defer file.Close()

	var num, errWrite = file.Write(content)
	if errWrite != nil {
		return num, errWrite
	}

	return num, nil
}

// DeleteFile removes a file from a node's storage
func DeleteFile(key, nodeKey string) {
	os.Remove(ObtainFilePath(key, nodeKey))
}

// Identifier operations and hashing functions

// ConvertHexString converts a hexadecimal string to a big.Int
func ConvertHexString(hexString string) (*big.Int, error) {
	var bytes, err = hex.DecodeString(hexString)
	if err != nil {
		return nil, err
	}
	var bigInt = new(big.Int)
	return bigInt.SetBytes(bytes), nil
}

const keySize = BITS_SIZE_RING

var two = big.NewInt(2)

// hashMod is the modulus for the hash space (2^BITS_SIZE_RING)
var hashMod = new(big.Int).Exp(two, big.NewInt(keySize), nil)

// HashBytes generates a big.Int hash from a byte slice using SHA-1
func HashBytes(b []byte) *big.Int {
	var hasher = sha1.New()
	hasher.Write(b)
	var bytes = hasher.Sum(nil)
	return new(big.Int).SetBytes(bytes)
}

// Hash generates a big.Int hash from a string
func Hash(elt string) *big.Int {
	return HashBytes([]byte(elt))
}

// Jump calculates (n + 2^fingerentry) mod 2^m
// Used for finger table calculations
func Jump(nodeIdentifier big.Int, fingerentry int) *big.Int {
	// Calculate 2^fingerentry
	var fingerentryBig = big.NewInt(int64(fingerentry))
	var jump = new(big.Int).Exp(two, fingerentryBig, nil)

	// Calculate n + 2^fingerentry
	var sum = new(big.Int).Add(&nodeIdentifier, jump)

	// Take modulo 2^m
	return new(big.Int).Mod(sum, hashMod)
}

// Between checks if an element falls between two points in the identifier circle
func Between(start, elt, end *big.Int, inclusive bool) bool {
	if end.Cmp(start) > 0 {
		return (start.Cmp(elt) < 0 && elt.Cmp(end) < 0) || (inclusive && elt.Cmp(end) == 0)
	} else {
		return start.Cmp(elt) < 0 || elt.Cmp(end) < 0 || (inclusive && elt.Cmp(end) == 0)
	}
}

// Key management and network functions

// ObtainKeyBytes reads a key file from disk
func ObtainKeyBytes(keyPath string) ([]byte, error) {
	var content, errRead = os.ReadFile(keyPath)
	if errRead != nil {
		return nil, errRead
	}
	return content, nil
}

// ObtainPubKey loads and parses an RSA public key from a file
func ObtainPubKey(path string) (*rsa.PublicKey, error) {
	var content, errRead = ObtainKeyBytes(path)
	if errRead != nil {
		return nil, errRead
	}

	var pemBlock, _ = pem.Decode(content)
	if pemBlock == nil {
		return nil, errors.New("rsa public key could not be extracted from pem block")
	}

	var pub, errParse = x509.ParsePKIXPublicKey(pemBlock.Bytes)
	if errParse != nil {
		return nil, errParse
	}
	var pubKey, ok = pub.(*rsa.PublicKey)
	if !ok {
		return nil, errors.New("could not parse key to rsa key")
	}
	return pubKey, nil
}

// RPC Server and Handlers

// ChordRPCHandler handles RPC requests for the Chord node
type ChordRPCHandler int

// RpcServerInit initializes the RPC server for node communication
func RpcServerInit(l *net.Listener) {
	var handler = new(ChordRPCHandler)
	rpc.Register(handler)
	rpc.HandleHTTP()
	go http.Serve(*l, nil)
}

// RPC Handler Methods

// Predecessor handles RPC requests for node's predecessor information
func (t *ChordRPCHandler) Predecessor(empty string, reply *PredecessorRep) error {
	var node = Get()
	*reply = PredecessorRep{Predecessor: node.Predecessor}
	return nil
}

// Successors handles RPC requests for node's successor list
func (t *ChordRPCHandler) Successors(empty string, reply *SuccessorRep) error {
	var node = Get()
	*reply = SuccessorRep{Successors: node.Successors}
	return nil
}

// FindSuccessor handles RPC requests to find the successor for a given ID
func (t *ChordRPCHandler) FindSuccessor(args *big.Int, reply *FindSuccessorRep) error {
	var f, n = FindSuccessor(*args)
	*reply = FindSuccessorRep{Found: f, Node: n}
	return nil
}

// Notify handles RPC notifications from potential predecessors
func (t *ChordRPCHandler) Notify(args *NodeInfo, reply *string) error {
	Notify(*args)
	return nil
}

// KeyManager handles AES key generation and management
type KeyManager struct {
	KeyPath string
	NodeID  string // 添加节点ID标识
}

// NewKeyManager creates a new key manager instance
func NewKeyManager(nodeID string) *KeyManager {
	return &KeyManager{
		KeyPath: filepath.Join("./keys", nodeID, "aes.key"),
		NodeID:  nodeID,
	}
}

// GenerateKey generates a new AES-256 key if it doesn't exist
func (km *KeyManager) GenerateKey() error {
	// Check if key already exists
	if _, err := os.Stat(km.KeyPath); err == nil {
		return nil
	}

	// Create keys directory if it doesn't exist
	dir := filepath.Dir(km.KeyPath)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("failed to create key directory: %w", err)
	}

	// Generate new key
	key := make([]byte, AES_KEY_SIZE)
	if _, err := rand.Read(key); err != nil {
		return fmt.Errorf("failed to generate key: %w", err)
	}

	// Save key to file
	return os.WriteFile(km.KeyPath, key, KEY_PERMISSIONS)
}

// LoadKey loads the AES key from file
func (km *KeyManager) LoadKey() ([]byte, error) {
	key, err := os.ReadFile(km.KeyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load key: %w", err)
	}
	if len(key) != AES_KEY_SIZE {
		return nil, fmt.Errorf("invalid key size")
	}
	return key, nil
}

// FileEncryptor handles file encryption operations using AES
type FileEncryptor struct {
	keyManager *KeyManager
}

// NewFileEncryptor creates a new file encryptor instance
func NewFileEncryptor(km *KeyManager) *FileEncryptor {
	return &FileEncryptor{keyManager: km}
}

// EncryptFile encrypts a file using AES-256 in GCM mode
func (fe *FileEncryptor) EncryptFile(content []byte) ([]byte, error) {
	// Load encryption key
	key, err := fe.keyManager.LoadKey()
	if err != nil {
		return nil, fmt.Errorf("failed to load encryption key: %w", err)
	}

	// Create AES cipher
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	// Create GCM mode
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	// Generate random nonce
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	// Encrypt content
	// Final format: nonce + encrypted content
	encryptedContent := gcm.Seal(nonce, nonce, content, nil)
	return encryptedContent, nil
}

// DecryptFile decrypts a file encrypted with EncryptFile
func (fe *FileEncryptor) DecryptFile(encryptedContent []byte) ([]byte, error) {
	// Load encryption key
	key, err := fe.keyManager.LoadKey()
	if err != nil {
		return nil, fmt.Errorf("failed to load encryption key: %w", err)
	}

	// Create AES cipher
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	// Create GCM mode
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	// Extract nonce and ciphertext
	nonceSize := gcm.NonceSize()
	if len(encryptedContent) < nonceSize {
		return nil, fmt.Errorf("encrypted content too short")
	}

	nonce := encryptedContent[:nonceSize]
	ciphertext := encryptedContent[nonceSize:]

	// Decrypt content
	content, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt: %w", err)
	}

	return content, nil
}

// StoreFile handles RPC requests to store a file on this node
func (t *ChordRPCHandler) StoreFile(args *StoreFileArgs, reply *string) error {
	var key = args.Key.String()
	fmt.Printf("[rpcServerStoreFile] save file %v, content length %v", key, len(args.Content))
	var nodeKey = Get().Info.Identifier
	return NodeFileWrite(key, nodeKey.String(), args.Content)
}

// FileTransfer handles RPC requests to transfer multiple files
func (t *ChordRPCHandler) FileTransfer(args *TransferFileArgs, reply *string) error {
	fmt.Printf("[rpcServerTransferFile] save %v number of files", len(args.Files))
	var nodeKey = Get().Info.Identifier
	var errs = NodeFilesWrite(nodeKey.String(), args.Files)
	if len(errs) > 0 {
		return errors.New("could not write the files")
	}
	return nil
}

// MissingFiles handles RPC requests to identify missing files
func (t *ChordRPCHandler) MissingFiles(args *[]string, reply *[]string) error {
	var nodeKey = Get().Info.Identifier
	var files, err = ListFiles(nodeKey.String())
	if err != nil {
		return err
	}
	var temp = ObtainExclusive(*args, files)
	*reply = temp
	return nil
}

// CheckAlive handles RPC requests to check if node is responsive
func (t *ChordRPCHandler) CheckAlive(empty string, reply *bool) error {
	*reply = true
	return nil
}

// ObtainExclusive is a generic function to find elements in arr1 that are not in arr2
func ObtainExclusive[T comparable](arr1, arr2 []T) []T {
	var exclusive = []T{}
	for _, item1 := range arr1 {
		var found = false
		for _, item2 := range arr2 {
			if item1 == item2 {
				found = true
				break
			}
		}
		if !found {
			exclusive = append(exclusive, item1)
		}
	}
	return exclusive
}

// RPC Client Functions

// getConnection establishes an RPC connection to a node
func getConnection(nodeAddress NodeAddress) (*rpc.Client, error) {
	var client, err = rpc.DialHTTP("tcp", string(nodeAddress))
	return client, err
}

// handleCall is a generic function to handle RPC calls
func handleCall[ArgT, RepT any](nodeAddress NodeAddress, method string, args *ArgT, reply *RepT) error {
	var client, err = getConnection(nodeAddress)
	if err != nil {
		return err
	}
	defer client.Close() // Ensure connection is closed
	return client.Call(method, args, reply)
}

// Predecessor retrieves predecessor information from a remote node
func Predecessor(node NodeAddress) (*NodeInfo, error) {
	var reply PredecessorRep
	var dummyArg = "empty"
	var err = handleCall(node, "ChordRPCHandler.Predecessor", &dummyArg, &reply)
	if err != nil {
		return nil, err
	}
	return reply.Predecessor, nil
}

// Successors retrieves successor list from a remote node
func Successors(node NodeAddress) ([]NodeInfo, error) {
	var reply SuccessorRep
	var dummyArg = "empty"
	var err = handleCall(node, "ChordRPCHandler.Successors", &dummyArg, &reply)
	if err != nil {
		return nil, err
	}
	return reply.Successors, err
}

// ClientLocateSuccessor requests successor information for an ID from a remote node
func ClientLocateSuccessor(node NodeAddress, id *big.Int) (*FindSuccessorRep, error) {
	var reply FindSuccessorRep
	var err = handleCall(node, "ChordRPCHandler.FindSuccessor", id, &reply)
	if err != nil {
		return nil, err
	}
	return &reply, nil
}

// ClientNotification sends a notification to a remote node
func ClientNotification(notifiee NodeAddress, notifier NodeInfo) error {
	var reply string
	var err = handleCall(notifiee, "ChordRPCHandler.Notify", &notifier, &reply)
	return err
}

// StoreFileClient sends a request to store a file on a remote node
func StoreFileClient(nodeAddress NodeAddress, fileKey big.Int, content []byte) error {
	// 确保只对远程节点使用 RPC
	var currentNode = Get()
	if string(nodeAddress) == fmt.Sprintf("%v:%v", currentNode.Info.IpAddress, currentNode.Info.ChordPort) {
		return NodeFileWrite(fileKey.String(), currentNode.Info.Identifier.String(), content)
	}

	var reply string
	var args = StoreFileArgs{Key: fileKey, Content: content}
	var err = handleCall(nodeAddress, "ChordRPCHandler.StoreFile", &args, &reply)
	return err
}

// FileTransfer sends multiple files to a remote node
func FileTransfer(nodeAddress NodeAddress, files map[string]*[]byte) error {
	var reply string
	var args = TransferFileArgs{Files: files}
	var err = handleCall(nodeAddress, "ChordRPCHandler.FileTransfer", &args, &reply)
	return err
}

// MissingFiles requests list of missing files from a remote node
func MissingFiles(nodeAddress NodeAddress, files []string) ([]string, error) {
	var reply []string
	var err = handleCall(nodeAddress, "ChordRPCHandler.MissingFiles", &files, &reply)
	return reply, err
}

// CheckAlive checks if a remote node is responsive
func CheckAlive(nodeAddress NodeAddress) bool {
	var dummy = "empty"
	var reply bool
	var err = handleCall(nodeAddress, "ChordRPCHandler.CheckAlive", &dummy, &reply)
	return err == nil && reply
}

// FunctionSchedule is a type for periodic maintenance functions
type FunctionSchedule func()

// Schedule runs a maintenance function periodically
func Schedule(function FunctionSchedule, t time.Duration) {
	go func() {
		for {
			time.Sleep(t)
			function()
		}
	}()
}
