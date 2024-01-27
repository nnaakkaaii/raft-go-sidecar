package raft

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

func mapcopy[T1 comparable, T2 any](src map[T1]T2) map[T1]T2 {
	dst := map[T1]T2{}
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
