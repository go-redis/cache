package limitget

// The module is designed for limiting concurrency of getting data from database.
// limitget is designed limit the number of Get

// MaxConcurrency represents the total number of Get with all kinds of key.
var MaxConcurrency = 1000

type Chans struct {
	MaxConCh chan uint8 //The channel is used for control the max concurrency number of get data from database
}

func (chs *Chans) LimitGet() {
	chs.MaxConCh <- uint8(1)
}

func (chs *Chans) ReleaseGet() {
	<-chs.MaxConCh
}
