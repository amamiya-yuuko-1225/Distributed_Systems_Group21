/*
 * @Author: amamiya-yuuko-1225 1913250675@qq.com
 * @Date: 2024-11-09 14:27:05
 * @LastEditors: amamiya-yuuko-1225 1913250675@qq.com
 * @Description:
 */
package main

import (
	"bufio"
	"bytes"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	//maxmimum number of connections
	MAX_CONN = 10
	//root directory for files
	FILE_DIR = "."
)

var (
	//map file extenstions to content-type
	extMap = map[string]string{
		".html": "text/html",
		".txt":  "text/plain",
		".gif":  "image/gif",
		".jpeg": "image/jpeg",
		".jpg":  "image/jpeg",
		".css":  "text/css",
	}
	// control the degree of accepted connections
	connChan = make(chan int, MAX_CONN)

	//reponse when internal error occured
	internalErrorResponse = &http.Response{
		Status:     "500 Internal Server Error",
		StatusCode: http.StatusInternalServerError,
		Proto:      "HTTP/1.0",
		ProtoMajor: 1,
		ProtoMinor: 0,
		Header:     make(http.Header),
		Body:       nil,
	}
	//reponse when invalid resquest received
	badReqResponse = &http.Response{
		Status:     "400 Bad Request",
		StatusCode: http.StatusBadRequest,
		Proto:      "HTTP/1.0",
		ProtoMajor: 1,
		ProtoMinor: 0,
		Header:     make(http.Header),
		Body:       nil,
	}
)

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
 * @description: check if the file in "GET" or "POST" method has valid extenstion
 * @param {string} path: path of the file
 * @return {bool, string}:
 * if valid, return true, contentType
 * else, return false, ""
 */
func check_ext_validity(path string) (bool, string) {
	ext := filepath.Ext(path)

	//Check if file extension supported
	contentType, ok := extMap[ext]
	if !ok {
		return false, ""
	} else {
		return true, contentType
	}
}

/**
 * @description: process GET request
 * @param {*http.Request} req
 * @return {*http.Response}:
 * 500 internal error; 400 Bad Request for unsupported file extensions
 * 404 Not Found for missing file; 200 OK
 */
func process_get_req(req *http.Request) *http.Response {
	//Get request path and file extension
	path := req.URL.Path

	//Check if extension valid. If valid, get contentType; else return
	//400 Bad Request
	valid, contentType := check_ext_validity(path)
	if !valid {
		return badReqResponse
	}

	//Open target file
	file, err := os.Open(FILE_DIR + path)
	if err != nil {
		//file do not exists
		if _, ok := err.(*os.PathError); ok {
			return &http.Response{
				Status:     "404 Not Found",
				StatusCode: http.StatusNotFound,
				Proto:      "HTTP/1.0",
				ProtoMajor: 1,
				ProtoMinor: 0,
				Header:     make(http.Header),
				Body:       nil,
			}
		} else {
			log.Println("File open failed: ", err)
			return internalErrorResponse
		}
	}
	defer file.Close()

	//Read target file
	data, err := io.ReadAll(file)
	if err != nil {
		log.Println("Read file failed: ", err)
		return internalErrorResponse
	}

	// Request successfully handled
	response := &http.Response{
		Status:     "200 OK",
		StatusCode: http.StatusOK,
		Proto:      "HTTP/1.0",
		ProtoMajor: 1,
		ProtoMinor: 0,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader(data)),
	}
	response.Header.Set("Content-Type", contentType)
	return response
}

/**
 * @description: process POST request
 * @param {*http.Request} req
 * @return {*http.Response} 501 internal error; 201 Created
 */
func process_post_req(req *http.Request) *http.Response {
	//Open requst body
	reqBody := req.Body
	defer reqBody.Close()

	//Get request path
	path := req.URL.Path

	//Check if extension valid. If not valid return 400 Bad Request
	valid, _ := check_ext_validity(path)
	if !valid {
		return badReqResponse
	}

	//Create an empty file for the target
	outFile, err := os.Create(FILE_DIR + path)
	if err != nil {
		log.Println("File creation failed: ", err)
		return internalErrorResponse
	}
	defer outFile.Close()

	//Write request body to target file
	if _, err := io.Copy(outFile, reqBody); err != nil {
		log.Println("Failed to write request body to target file: ", err)
		return internalErrorResponse
	}

	// POST successfully handled, resource created
	return &http.Response{
		Status:     "201 Created",
		StatusCode: http.StatusCreated,
		Proto:      "HTTP/1.0",
		ProtoMajor: 1,
		ProtoMinor: 0,
		Header:     make(http.Header),
		Body:       nil,
	}
}

/**
 * @description: generate response for specified conn
 * @param {net.Conn} conn
 * @return {*http.Response}
 */
func generate_response(conn net.Conn) *http.Response {
	// input buffer
	buf := make([]byte, 1024)
	_, err := conn.Read(buf) // read from connection
	if err != nil {
		log.Println("Read from conn failed: ", err)
		return internalErrorResponse
	}
	// conver byte[] to bufio.reader
	reader := bufio.NewReader(strings.NewReader(string(buf)))
	// parse request
	req, err := http.ReadRequest(reader)
	if err != nil {
		log.Println("Read request failed: ", err)
		return internalErrorResponse
	}

	// Only handle "GET" & "POST" request
	// Otherwise respond "501 Not Implemented"
	switch req.Method {
	case "GET":
		return process_get_req(req)
	case "POST":
		return process_post_req(req)
	default:
		return &http.Response{
			Status:     "501 Not Implemented",
			StatusCode: http.StatusNotImplemented,
			Proto:      "HTTP/1.0",
			ProtoMajor: 1,
			ProtoMinor: 0,
			Header:     make(http.Header),
			Body:       nil,
		}
	}
}

/**
 * @description: deal with http request for a tcp connection
 * @param {net.Conn} conn: tcp connection
 * @return {*}
 */
func process_conn(conn net.Conn) {
	// to limit the number of connections using channel
	// if channel if full, the goroutine will be blocked
	connChan <- 1

	// add artifial delay to test max number of conn
	// FOR DEMO ONLY
	test_max_conn()

	// Decrement the connection count when done
	defer func() { <-connChan }()

	defer conn.Close() // close connection before exit

	//generate response, and write it to conn
	err := generate_response(conn).Write(conn)
	if err != nil {
		log.Println("Write to conn failed: ", err)
	}

}

func main() {

	//Get specified port from cmd, default 20000
	port := flag.String("p", "20000", "Specifiy listing port")
	flag.Parse()

	//Open listing socket
	listen, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatal("FATAL: Listening failed: ", err)
		return
	}
	defer listen.Close()

	//Always listening
	for {
		conn, err := listen.Accept() // Establish tcp connection
		if err != nil {
			log.Println("Accept new conn failed, skip this conn: ", err)
			continue
		}
		go process_conn(conn) // Create new connection socket in a new goroutine
	}
}
