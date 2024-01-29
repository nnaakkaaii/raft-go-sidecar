package raft

import peerv1 "github.com/nnaakkaaii/raft-gochannel/proto/peer/v1"

func synch[T any](
	initialValue T,
	valueCh <-chan T,
	getValueCh chan chan T,
	valueChangedCh chan<- T,
) {
	value := initialValue
	for {
		select {
		case newValue := <-valueCh:
			value = newValue
			valueChangedCh <- value
		case respCh := <-getValueCh:
			respCh <- value
		}
	}
}

func containsPeer(configPeers []*peerv1.Peer, id int32) bool {
	for _, peer := range configPeers {
		if peer.Id == id {
			return true
		}
	}
	return false
}
