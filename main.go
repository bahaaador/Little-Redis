package main

import (
	"fmt"
	"net"
	"strings"
)

var store = make(map[string]string)

func main() {
	listener, err := net.Listen("tcp", ":6379")
	if err != nil {
		fmt.Println("Error listening:", err)
		return
	}
	defer listener.Close()

	fmt.Println("Mini Redis server started on port 6379")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		// Read client command
		buffer := make([]byte, 1024)
		n, err := conn.Read(buffer)
		if err != nil {
			fmt.Println("Error reading:", err)
			return
		}

		command := string(buffer[:n])
		fmt.Printf("Received command: %s", command)

		parts := strings.Split(strings.TrimSpace(command), " ")
		switch parts[0] {
		case "SET":
			response := executeSet(parts)
			conn.Write([]byte(response + "\n"))
		case "GET":
			response := executeGet(parts)
			conn.Write([]byte(response + "\n"))
		case "DEL":
			response := executeDel(parts)
			conn.Write([]byte(response + "\n"))
		case "EXISTS":
			response := executeExists(parts)
			conn.Write([]byte(response + "\n"))
		case "KEYS":
			response := executeKeys(parts)
			conn.Write([]byte(response + "\n"))
		default:
			conn.Write([]byte("ERR unknown command '" + parts[0] + "'\n"))
		}
	}
}

func executeSet(parts []string) string {
	if len(parts) != 3 {
		return "ERR wrong number of arguments for 'set' command"
	}

	key := parts[1]
	value := parts[2]
	store[key] = value
	return "OK"
}

func executeGet(parts []string) string {
	if len(parts) != 2 {
		return "ERR wrong number of arguments for 'get' command"
	}

	key := parts[1]
	value, ok := store[key]
	if !ok {
		return "(nil)"
	}
	return value
}

func executeDel(parts []string) string {
	if len(parts) < 2 {
		return "ERR wrong number of arguments for 'del' command"
	}

	var count int
	for _, key := range parts[1:] {
		_, ok := store[key]
		if ok {
			delete(store, key)
			count++
		}
	}

	return fmt.Sprintf(":%d", count)
}

func executeExists(parts []string) string {
	if len(parts) < 2 {
		return "ERR wrong number of arguments for 'exists' command"
	}

	var count int
	for _, key := range parts[1:] {
		_, ok := store[key]
		if ok {
			count++
		}
	}

	return fmt.Sprintf(":%d", count)
}

func executeKeys(parts []string) string {
	if len(parts) != 2 {
		return "ERR wrong number of arguments for 'keys' command"
	}

	pattern := parts[1]
	var keys []string
	for key := range store {
		if match(key, pattern) {
			keys = append(keys, key)
		}
	}

	return "*" + strings.Join(keys, "\n")
}

func match(key, pattern string) bool {
	// Simple pattern matching implementation
	// Supports '*' as a wildcard
	if pattern == "*" {
		return true
	}
	return key == pattern
}
