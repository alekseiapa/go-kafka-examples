package main

import (
	"fmt"
	"log"

	"google.golang.org/protobuf/proto"
)

// Import the generated Protobuf struct
// This assumes the generated `person.pb.go` is in the same package
import "path/to/your/generated/personpb"

// serializePerson serializes a `Person` struct using Protocol Buffers.
func serializePerson(person *personpb.Person) ([]byte, error) {
	// Use the ProtoBuf `Marshal` function to serialize the struct into bytes.
	return proto.Marshal(person)
}

// deserializePerson deserializes a byte slice into a `Person` struct.
func deserializePerson(data []byte, person *personpb.Person) error {
	// Use the ProtoBuf `Unmarshal` function to convert bytes back into a struct.
	return proto.Unmarshal(data, person)
}

func main() {
	// Example struct to serialize
	p := &personpb.Person{
		Name:  "John Doe",
		Id:    12345,
		Email: "johndoe@example.com",
	}

	// Display original struct
	fmt.Println("Original struct:", p)

	// Serialize `Person` struct to Protocol Buffers binary format
	data, err := serializePerson(p)
	if err != nil {
		log.Fatalf("Failed to serialize person: %v", err)
	}

	// Display serialized data length
	fmt.Printf("Serialized data: %d bytes\n", len(data))

	// Deserialize the data back into a `Person` struct
	newPerson := &personpb.Person{}
	if err := deserializePerson(data, newPerson); err != nil {
		log.Fatalf("Failed to deserialize person: %v", err)
	}

	// Display the deserialized struct
	fmt.Println("Deserialized struct:", newPerson)

	// Check if the original and deserialized structs match
	if proto.Equal(p, newPerson) {
		fmt.Println("Success! The deserialized person matches the original.")
	} else {
		fmt.Println("Mismatch! The deserialized person does not match the original.")
	}
}
