package limitget

// The module is designed for limiting concurrency of getting data from database.
// limitget is designed limit the number of Get

// MaxConcurrency represents the total number of Get with all kinds of key.
var MaxConcurrency = 1000

type Chans struct {
	maxConCh chan uint8 //The channel is used for control the max concurrency number of get data from database
}

func (chs *Chans) LimitGet(key string) {
	chs.maxConCh <- uint8(1)
}

func (chs *Chans) ReleaseGet(key string) {
	<-chs.maxConCh
}
