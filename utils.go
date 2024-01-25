package main

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
