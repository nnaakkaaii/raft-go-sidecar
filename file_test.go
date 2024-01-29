package raft

import (
	"context"
	"os"
	"reflect"
	"testing"
)

func TestFileStorage(t *testing.T) {
	// Setup
	filePath := "test.db"
	defer os.Remove(filePath) // Clean up file after test

	fs := NewFileStorage(filePath)
	err := fs.Open()
	if err != nil {
		t.Fatalf("Failed to open file storage: %v", err)
	}
	defer fs.Close()

	// Create a state to save
	stateToSave := &State{
		CurrentTerm:  1,
		VotedFor:     2,
		LastLogTerm:  3,
		LastLogIndex: 4,
	}

	// Save the state
	ctx := context.Background()
	fs.SaveState(ctx, stateToSave)

	// Load the state
	loadedState := &State{}
	fs.LoadState(ctx, loadedState)

	// Verify
	if !reflect.DeepEqual(loadedState.CurrentTerm, stateToSave.CurrentTerm) ||
		!reflect.DeepEqual(loadedState.VotedFor, stateToSave.VotedFor) ||
		!reflect.DeepEqual(loadedState.LastLogTerm, stateToSave.LastLogTerm) ||
		!reflect.DeepEqual(loadedState.LastLogIndex, stateToSave.LastLogIndex) {
		t.Errorf("Loaded state does not match saved state")
	}
}
