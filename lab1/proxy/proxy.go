/*
 * @Author: amamiya-yuuko-1225 1913250675@qq.com
 * @Date: 2024-11-09
 * @Description: Proxy server implementation
 */
package main

import (
	"bufio"
	"bytes"
	"flag"
	"log"
	"net"
	"net/http"
	"time"
)

const MAX_CONN = 10 // Define the maximum number of concurrent connections

// connChan is a buffered channel used to limit the number of concurrent connections
var connChan = make(chan int, MAX_CONN)

// when unexpected error occured, return 500 internal error
var internalErrorResponse = &http.Response{
	Status:     "500 Internal Server Error",
	StatusCode: http.StatusInternalServerError,
	Proto:      "HTTP/1.0",
	ProtoMajor: 1,
	ProtoMinor: 0,
	Header:     make(http.Header),
	Body:       nil,
}

/**
 * @description: add artifial delay to test max number of conn
 * @return {*}
 */
func test_max_conn() {
	// show the number of connections
	log.Println("Number of connection", len(connChan))
	// add artificial delay to demonstrate max number of connections
	time.Sleep(20 * time.Millisecond)
}

/**
 * @description: Handles proxying requests from client to remote server and relaying the response.
 * @param {net.Conn} conn: TCP connection with the client.
 */
func handleProxyConnection(conn net.Conn) {
	// Increment the connection count by sending a signal to the channel
	connChan <- 1

	// add artifial delay to test max number of conn
	// FOR DEMO ONLY
	test_max_conn()

	defer func() { <-connChan }() // Decrement the connection count when done
	defer conn.Close()            // Ensure the connection is closed when function exits

	// Read the incoming data from the client into a buffer
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		// Log and return if an error occurs while reading the data
		log.Println("Error reading from connection:", err)
		internalErrorResponse.Write(conn)
		return
	}

	// Parse the HTTP request from the buffer
	reader := bufio.NewReader(bytes.NewReader(buf[:n]))
	req, err := http.ReadRequest(reader)
	if err != nil {
		// Log and return if there's an error parsing the request
		log.Println("Error parsing request:", err)
		internalErrorResponse.Write(conn)
		return
	}

	// Check if the HTTP method is not GET (only GET is supported)
	if req.Method != "GET" {
		// Respond with a 501 Not Implemented error if the method is not GET
		response := &http.Response{
			Status:     "501 Not Implemented",
			StatusCode: http.StatusNotImplemented,
			Proto:      "HTTP/1.0",
			ProtoMajor: 1,
			ProtoMinor: 0,
			Header:     make(http.Header),
			Body:       nil,
		}
		// Write the error response to the connection
		_ = response.Write(conn)
		return
	}

	// Create a TCP connection to the target server (remote server)
	targetConn, err := net.Dial("tcp", req.Host)
	if err != nil {
		// Log and return if an error occurs while connecting to the target server
		log.Println("Error connecting to target server:", err)
		// Respond with a 502 Bad Gateway error if connection fails
		response := &http.Response{
			Status:     "502 Bad Gateway",
			StatusCode: http.StatusBadGateway,
			Proto:      "HTTP/1.0",
			ProtoMajor: 1,
			ProtoMinor: 0,
			Header:     make(http.Header),
			Body:       nil,
		}
		_ = response.Write(conn) // Send error response to client
		return
	}
	defer targetConn.Close() // Ensure the target connection is closed when function exits

	// Forward the request to the target server
	err = req.Write(targetConn)
	if err != nil {
		// Log and return if an error occurs while sending the request to the target server
		log.Println("Error forwarding request to target server:", err)
		internalErrorResponse.Write(conn)
		return
	}

	// Read the response from the target server
	resp, err := http.ReadResponse(bufio.NewReader(targetConn), req)
	if err != nil {
		// Log and return if an error occurs while reading the response from the target server
		log.Println("Error reading response from target server:", err)
		internalErrorResponse.Write(conn)
		return
	}

	// Forward the response from the target server back to the client
	err = resp.Write(conn)
	if err != nil {
		// Log and return if an error occurs while sending the response back to the client
		log.Println("Error forwarding response to client:", err)
		return
	}
}

/**
 * @description: Start the proxy server on the specified port.
 * @param {string} port: Port to listen on.
 */
func startProxyServer(port string) {
	// Set up a listener to accept incoming TCP connections on the specified port
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		// Log and terminate the program if there's an error starting the server
		log.Fatal("Error starting proxy server:", err)
	}
	defer listener.Close() // Ensure the listener is closed when the function exits

	// Log the proxy server start message
	log.Println("Proxy server is running on port", port)

	// Infinite loop to keep accepting new client connections
	for {
		// Accept a new connection from a client
		conn, err := listener.Accept()
		if err != nil {
			// Log and continue to the next connection if there's an error accepting the connection
			log.Println("Error accepting connection:", err)
			continue
		}
		// Handle the connection in a new goroutine to allow multiple concurrent connections
		go handleProxyConnection(conn)
	}
}

func main() {
	// Define the command-line flag for the port with a default value of "30000"
	port := flag.String("p", "30000", "Specify listening port for the proxy server")
	flag.Parse() // Parse the command-line flags

	// Start the proxy server on the specified port
	startProxyServer(*port)
}
