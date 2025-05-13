package dht

import (
	"context"
	"fmt"
	gocid "github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-kad-dht/amino"
	"github.com/libp2p/go-libp2p-kad-dht/sr"
	kspace "github.com/libp2p/go-libp2p-kbucket/keyspace"
	mh "github.com/multiformats/go-multihash"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-kad-dht/internal"
	"github.com/libp2p/go-libp2p-kad-dht/metrics"
	"github.com/libp2p/go-libp2p-kad-dht/qpeerset"
	kb "github.com/libp2p/go-libp2p-kbucket"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"go.opentelemetry.io/otel/trace"
)

// GetAllClosestPeers  returns the k closest nodes to the given key, and
// additionally returns all nodes inside the query queue.
//
// This function was separated from the the normal GetClosestPeers for debugging.
func (dht *IpfsDHT) GetAllClosestPeers(ctx context.Context, key string) ([]peer.ID, *qpeerset.QueryPeerset, error) {
	ctx, span := internal.StartSpan(ctx, "IpfsDHT.GetClosestPeers", trace.WithAttributes(internal.KeyAsAttribute("Key", key)))
	defer span.End()

	if key == "" {
		return nil, nil, fmt.Errorf("can't lookup empty key")
	}

	//TODO: I can break the interface! return []peer.ID
	lookupRes, err := dht.runLookupWithFollowup(ctx, key, dht.pmGetClosestPeers(key), func(*qpeerset.QueryPeerset) bool { return false })

	if err != nil {
		return nil, nil, err
	}

	if err := ctx.Err(); err != nil || !lookupRes.completed {
		return lookupRes.peers, nil, err
	}

	// tracking lookup results for network size estimator
	if err = dht.nsEstimator.Track(key, lookupRes.closest); err != nil {
		logger.Warnf("network size estimator track peers: %s", err)
	}

	if ns, err := dht.nsEstimator.NetworkSize(); err == nil {
		metrics.NetworkSize.M(int64(ns))
	}

	// refresh the cpl for this key as the query was successful
	dht.routingTable.ResetCplRefreshedAtForID(kb.ConvertKey(key), time.Now())

	return lookupRes.peers, lookupRes.allQueryPeers, nil
}

// GetClosestPeers is a Kademlia 'node lookup' operation. Returns a channel of
// the K closest peers to the given key.
//
// If the context is canceled, this function will return the context error along
// with the closest K peers it has found so far.
func (dht *IpfsDHT) GetClosestPeers(ctx context.Context, key string) ([]peer.ID, error) {
	ctx, span := internal.StartSpan(ctx, "IpfsDHT.GetClosestPeers", trace.WithAttributes(internal.KeyAsAttribute("Key", key)))
	defer span.End()

	if key == "" {
		return nil, fmt.Errorf("can't lookup empty key")
	}

	//TODO: I can break the interface! return []peer.ID
	lookupRes, err := dht.runLookupWithFollowup(ctx, key, dht.pmGetClosestPeers(key), func(*qpeerset.QueryPeerset) bool { return false })

	if err != nil {
		return nil, err
	}

	if err := ctx.Err(); err != nil || !lookupRes.completed {
		return lookupRes.peers, err
	}

	// tracking lookup results for network size estimator
	if err = dht.nsEstimator.Track(key, lookupRes.closest); err != nil {
		logger.Warnf("network size estimator track peers: %s", err)
	}

	if ns, err := dht.nsEstimator.NetworkSize(); err == nil {
		metrics.NetworkSize.M(int64(ns))
	}

	// refresh the cpl for this key as the query was successful
	dht.routingTable.ResetCplRefreshedAtForID(kb.ConvertKey(key), time.Now())

	return lookupRes.peers, nil
}

// pmGetClosestPeers is the protocol messenger version of the GetClosestPeer queryFn.
func (dht *IpfsDHT) pmGetClosestPeers(key string) queryFn {
	return func(ctx context.Context, p peer.ID) ([]*peer.AddrInfo, error) {
		// For DHT query command
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type: routing.SendingQuery,
			ID:   p,
		})

		peers, err := dht.protoMessenger.GetClosestPeers(ctx, p, peer.ID(key))
		if err != nil {
			logger.Debugf("error getting closer peers: %s", err)
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type:  routing.QueryError,
				ID:    p,
				Extra: err.Error(),
			})
			return nil, err
		}

		// For DHT query command
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type:      routing.PeerResponse,
			ID:        p,
			Responses: peers,
		})

		return peers, err
	}
}

// GetClosestPeersAndProvideIfWithinDk performs a normal lookup towards the content,
// however it provides if the contacted node is within the distance $d_k$. The list of
// closest peers returned contains the closest k peers which did not yet received the
// provider record.
func (dht *IpfsDHT) GetClosestPeersAndProvideIfWithinDk(ctx context.Context, key string) ([]peer.ID, []peer.ID, error) {
	ctx, span := internal.StartSpan(ctx, "IpfsDHT.GetClosestPeers", trace.WithAttributes(internal.KeyAsAttribute("Key", key)))
	defer span.End()

	if key == "" {
		return nil, nil, fmt.Errorf("can't lookup empty key")
	}

	providedTo := sync.Map{}
	// queried := sync.Map{}

	//TODO: I can break the interface! return []peer.ID
	lookupRes, err := dht.runLookupWithFollowup(ctx, key, dht.pmGetClosestPeersAndProvideIfWithinDk(key, &providedTo), func(*qpeerset.QueryPeerset) bool { return false })
	if err != nil {
		return nil, nil, err
	}

	// i := 1
	// fmt.Println("Distance:", ToSciNotation(dht.KDistance.GetAverage(sr.WeightedMean)))
	// providedTo.Range(func(key, value interface{}) bool {
	// 	fmt.Printf("\t[%d] key: %v\n", i, key)
	// 	i++
	// 	return true
	// })

	// Get all the peers obtained from the lookup
	allQueriedPeers := lookupRes.allQueryPeers.GetClosestInStates(qpeerset.PeerHeard, qpeerset.PeerQueried, qpeerset.PeerWaiting)
	// fmt.Println("Queried:")
	// for i2, queriedPeer := range allQueriedPeers {
	// 	fmt.Printf("\t[%d] queried: %v\n", i2, queriedPeer)
	// }

	// Get the closest k excluding the ones that already received the record
	var notProvidedClosest []peer.ID
	for i := 0; len(notProvidedClosest) < amino.DefaultBucketSize; i++ {
		if _, ok := providedTo.Load(allQueriedPeers[i]); !ok {
			notProvidedClosest = append(notProvidedClosest, allQueriedPeers[i])
		}
	}

	// fmt.Println("NotProvidedKClosest:")
	// for i, notProvidedPeer := range notProvidedClosest {
	// 	fmt.Printf("\t[%d] queried: %v\n", i, notProvidedPeer)
	// }

	if err := ctx.Err(); err != nil || !lookupRes.completed {
		return lookupRes.peers, nil, err
	}

	// tracking lookup results for network size estimator
	if err = dht.nsEstimator.Track(key, lookupRes.closest); err != nil {
		logger.Warnf("network size estimator track peers: %s", err)
	}

	if ns, err := dht.nsEstimator.NetworkSize(); err == nil {
		metrics.NetworkSize.M(int64(ns))
	}

	// refresh the cpl for this key as the query was successful
	dht.routingTable.ResetCplRefreshedAtForID(kb.ConvertKey(key), time.Now())

	// Convert all the peers provided to an array
	var providedToArray []peer.ID
	providedTo.Range(func(key, _ interface{}) bool {
		providedToArray = append(providedToArray, key.(peer.ID))
		return true
	})

	return notProvidedClosest, providedToArray, nil
}

// pmGetClosestPeers is the protocol messenger version of the GetClosestPeer queryFn.
func (dht *IpfsDHT) pmGetClosestPeersAndProvideIfWithinDk(key string, providedTo *sync.Map) queryFn {
	return func(ctx context.Context, p peer.ID) ([]*peer.AddrInfo, error) {
		// For DHT query command
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type: routing.SendingQuery,
			ID:   p,
		})

		peers, err := dht.protoMessenger.GetClosestPeers(ctx, p, peer.ID(key))
		if err != nil {
			logger.Debugf("error getting closer peers: %s", err)
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type:  routing.QueryError,
				ID:    p,
				Extra: err.Error(),
			})
			return nil, err
		}

		cid, err := gocid.Cast([]byte(key))
		if err != nil {
			return nil, err
		}
		cidKey := kspace.XORKeySpace.Key(cid.Hash())

		peerMultiHash, _ := mh.FromB58String(p.String())
		peerKey := kspace.XORKeySpace.Key(peerMultiHash)

		provided := 0
		providedTo.Range(func(key, _ interface{}) bool {
			provided++
			return true
		})

		distance := peerKey.Distance(cidKey)
		if distance.Cmp(dht.KDistance.GetAverage(sr.WeightedMean)) <= 0 {
			log.Info.Printf("%d) PutPR(%s, %s)\n", provided+1, cid, p)

			err := dht.protoMessenger.PutProviderAddrs(ctx, p, cid.Hash(), peer.AddrInfo{
				ID:    dht.self,
				Addrs: dht.filterAddrs(dht.host.Addrs()),
			})
			if err != nil {
				// fmt.Println(err)
				// fmt.Println("Test")
			} else {
				providedTo.Store(p, true)
			}
		}

		// For DHT query command
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type:      routing.PeerResponse,
			ID:        p,
			Responses: peers,
		})

		return peers, err
	}
}
