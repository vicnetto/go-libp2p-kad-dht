package dht

import "sync"

var realProvider string

var prMutex sync.Mutex
var lookupPrProvider string
var lookupProvider string
var found bool
var ProvideFinished chan error = make(chan error, 10)

func SetRealProvider(provider string) {
	realProvider = provider
}

func ResetPRProvider() {
	prMutex.Lock()

	lookupPrProvider = ""
	lookupProvider = ""
	found = false

	prMutex.Unlock()
}

func SetPRProvider(prProvider string, provider string) {
	prMutex.Lock()

	if !found && len(realProvider) != 0 {
		if realProvider == provider {
			found = true
		}

		lookupPrProvider = prProvider
		lookupProvider = provider
	}

	prMutex.Unlock()
}

func GetLookupInformation() (bool, string) {
	return found, lookupPrProvider
}
