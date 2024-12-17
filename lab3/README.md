how to run 

use "go build" to generate a "ChordClient.exe" file, run this binary file with necessary arguments

basic instruction format: ./ChordClient [flags]

compulsory flags:
1. -a: The IP address that the Chord client will bind to, as well as advertise to other nodes. Represented as an ASCII string (e.g., 128.8.126.63). Must be specified.
2. -p: The port that the Chord client will bind to and listen on. Represented as a base-10 integer. Must be specified.
3. -ja: The IP address of the machine running a Chord node. The Chord client will join this nodes ring. Represented as an ASCII string (e.g., 128.8.126.63). Must be specified if --jp is specified.
4. -jp: The port that an existing Chord node is bound to and listening on. The Chord client will join this nodes ring. Represented as a base-10 integer. Must be specified if --ja is specified.
5. -sp: The port number for the SSH server. Must be specidfied
6. -ts: The port that an existing Chord node is bound to and listening on. The Chord client will join this nodes ring. Represented as a base-10 integer. Must be specified if --ja is specified.
7. -tff: The port that an existing Chord node is bound to and listening on. The Chord client will join this nodes ring. Represented as a base-10 integer. Must be specified if --ja is specified.
8. -tcp: The time in milliseconds between invocations of check predecessor.
9. -tb: The time duration for backup interval.Must be specified
10. -r: The number of successors maintained by the Chord client. Represented as a base-10 integer. Must be specified, with a value in the range of [1,32].
11. -i: The identifier (ID) assigned to the Chord client which will override the ID computed by the SHA1 sum of the client’s IP address and port number. Represented as a string of 40 characters matching [0-9a-fA-F]. Optional parameter.

instructions after running:
Lookup <filename> or l <filename>

StoreFile <filepath> [ssh] [encrypt] or s <filepath> [ssh] [encrypt] 
(use "s <filepath> f f" to disale encryption)

PrintState or p


HOW TO START
V2.0  12/16 updated 
show 8 terminals by Tmux now
////make sure to install Tmux first!
////check the username in main.go and startsfpt.sh

just use "./start.sh"
return to Tmux terminal and use "tmux kill-server" to exit




v1
example: 
//create the first node of a new ring
./ChordClient -a 127.0.0.1 -p 8001 -sp 2201 -ts 1000 -tff 1000 -tcp 1000 -tb 1000 -r 3

//create a second node to join the ring
./ChordClient -a 127.0.0.1 -p 8002 -sp 2202 -ts 1000 -tff 1000 -tcp 1000 -tb 1000 -r 3 -ja 127.0.0.1 -jp 8001

generate keys:
1. mkdir keys
2. ssh-keygen -t rsa -f keys/id_rsa -N "" 
//generate SSH key pair
3. openssl genrsa -out keys/encryption.pem 2048
//generate RSA key pair for encryption
4. openssl rsa -in keys/encryption.pem -pubout -out keys/encryption-pub.pem
//extract the public key from the private key
