package dht

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/routing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	u "github.com/ipfs/boxo/util"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-kad-dht/internal"
	internalConfig "github.com/libp2p/go-libp2p-kad-dht/internal/config"
	"github.com/libp2p/go-libp2p-kad-dht/netsize"
	"github.com/libp2p/go-libp2p-kad-dht/qpeerset"
	kb "github.com/libp2p/go-libp2p-kbucket"
	record "github.com/libp2p/go-libp2p-record"
	"github.com/multiformats/go-multihash"
)

// This file implements the Routing interface for the IpfsDHT struct.

// Basic Put/Get

// PutValue adds value corresponding to given Key.
// This is the top level "Store" operation of the DHT
func (dht *IpfsDHT) PutValue(ctx context.Context, key string, value []byte, opts ...routing.Option) (err error) {
	ctx, end := tracer.PutValue(dhtName, ctx, key, value, opts...)
	defer func() { end(err) }()

	if !dht.enableValues {
		return routing.ErrNotSupported
	}

	logger.Debugw("putting value", "key", internal.LoggableRecordKeyString(key))

	// don't even allow local users to put bad values.
	if err := dht.Validator.Validate(key, value); err != nil {
		return err
	}

	old, err := dht.getLocal(ctx, key)
	if err != nil {
		// Means something is wrong with the datastore.
		return err
	}

	// Check if we have an old value that's not the same as the new one.
	if old != nil && !bytes.Equal(old.GetValue(), value) {
		// Check to see if the new one is better.
		i, err := dht.Validator.Select(key, [][]byte{value, old.GetValue()})
		if err != nil {
			return err
		}
		if i != 0 {
			return fmt.Errorf("can't replace a newer value with an older value")
		}
	}

	rec := record.MakePutRecord(key, value)
	rec.TimeReceived = u.FormatRFC3339(time.Now())
	err = dht.putLocal(ctx, key, rec)
	if err != nil {
		return err
	}

	peers, err := dht.GetClosestPeers(ctx, key)
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	for _, p := range peers {
		wg.Add(1)
		go func(p peer.ID) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			defer wg.Done()
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type: routing.Value,
				ID:   p,
			})

			err := dht.protoMessenger.PutValue(ctx, p, rec)
			if err != nil {
				logger.Debugf("failed putting value to peer: %s", err)
			}
		}(p)
	}
	wg.Wait()

	return nil
}

// recvdVal stores a value and the peer from which we got the value.
type recvdVal struct {
	Val  []byte
	From peer.ID
}

// GetValue searches for the value corresponding to given Key.
func (dht *IpfsDHT) GetValue(ctx context.Context, key string, opts ...routing.Option) (result []byte, err error) {
	ctx, end := tracer.GetValue(dhtName, ctx, key, opts...)
	defer func() { end(result, err) }()

	if !dht.enableValues {
		return nil, routing.ErrNotSupported
	}

	// apply defaultQuorum if relevant
	var cfg routing.Options
	if err := cfg.Apply(opts...); err != nil {
		return nil, err
	}
	opts = append(opts, Quorum(internalConfig.GetQuorum(&cfg)))

	responses, err := dht.SearchValue(ctx, key, opts...)
	if err != nil {
		return nil, err
	}
	var best []byte

	for r := range responses {
		best = r
	}

	if ctx.Err() != nil {
		return best, ctx.Err()
	}

	if best == nil {
		return nil, routing.ErrNotFound
	}
	logger.Debugf("GetValue %v %x", internal.LoggableRecordKeyString(key), best)
	return best, nil
}

// SearchValue searches for the value corresponding to given Key and streams the results.
func (dht *IpfsDHT) SearchValue(ctx context.Context, key string, opts ...routing.Option) (ch <-chan []byte, err error) {
	ctx, end := tracer.SearchValue(dhtName, ctx, key, opts...)
	defer func() { ch, err = end(ch, err) }()

	if !dht.enableValues {
		return nil, routing.ErrNotSupported
	}

	var cfg routing.Options
	if err := cfg.Apply(opts...); err != nil {
		return nil, err
	}

	responsesNeeded := 0
	if !cfg.Offline {
		responsesNeeded = internalConfig.GetQuorum(&cfg)
	}

	stopCh := make(chan struct{})
	valCh, lookupRes := dht.getValues(ctx, key, stopCh)

	out := make(chan []byte)
	go func() {
		defer close(out)
		best, peersWithBest, aborted := dht.searchValueQuorum(ctx, key, valCh, stopCh, out, responsesNeeded)
		if best == nil || aborted {
			return
		}

		updatePeers := make([]peer.ID, 0, dht.bucketSize)
		select {
		case l := <-lookupRes:
			if l == nil {
				return
			}

			for _, p := range l.peers {
				if _, ok := peersWithBest[p]; !ok {
					updatePeers = append(updatePeers, p)
				}
			}
		case <-ctx.Done():
			return
		}

		dht.updatePeerValues(dht.Context(), key, best, updatePeers)
	}()

	return out, nil
}

func (dht *IpfsDHT) searchValueQuorum(ctx context.Context, key string, valCh <-chan recvdVal, stopCh chan struct{},
	out chan<- []byte, nvals int) ([]byte, map[peer.ID]struct{}, bool) {
	numResponses := 0
	return dht.processValues(ctx, key, valCh,
		func(ctx context.Context, v recvdVal, better bool) bool {
			numResponses++
			if better {
				select {
				case out <- v.Val:
				case <-ctx.Done():
					return false
				}
			}

			if nvals > 0 && numResponses > nvals {
				close(stopCh)
				return true
			}
			return false
		})
}

func (dht *IpfsDHT) processValues(ctx context.Context, key string, vals <-chan recvdVal,
	newVal func(ctx context.Context, v recvdVal, better bool) bool) (best []byte, peersWithBest map[peer.ID]struct{}, aborted bool) {
loop:
	for {
		if aborted {
			return
		}

		select {
		case v, ok := <-vals:
			if !ok {
				break loop
			}

			// Select best value
			if best != nil {
				if bytes.Equal(best, v.Val) {
					peersWithBest[v.From] = struct{}{}
					aborted = newVal(ctx, v, false)
					continue
				}
				sel, err := dht.Validator.Select(key, [][]byte{best, v.Val})
				if err != nil {
					logger.Warnw("failed to select best value", "key", internal.LoggableRecordKeyString(key), "error", err)
					continue
				}
				if sel != 1 {
					aborted = newVal(ctx, v, false)
					continue
				}
			}
			peersWithBest = make(map[peer.ID]struct{})
			peersWithBest[v.From] = struct{}{}
			best = v.Val
			aborted = newVal(ctx, v, true)
		case <-ctx.Done():
			return
		}
	}

	return
}

func (dht *IpfsDHT) updatePeerValues(ctx context.Context, key string, val []byte, peers []peer.ID) {
	fixupRec := record.MakePutRecord(key, val)
	for _, p := range peers {
		go func(p peer.ID) {
			// TODO: Is this possible?
			if p == dht.self {
				err := dht.putLocal(ctx, key, fixupRec)
				if err != nil {
					logger.Error("Error correcting local dht entry:", err)
				}
				return
			}
			ctx, cancel := context.WithTimeout(ctx, time.Second*30)
			defer cancel()
			err := dht.protoMessenger.PutValue(ctx, p, fixupRec)
			if err != nil {
				logger.Debug("Error correcting DHT entry: ", err)
			}
		}(p)
	}
}

func (dht *IpfsDHT) getValues(ctx context.Context, key string, stopQuery chan struct{}) (<-chan recvdVal, <-chan *lookupWithFollowupResult) {
	valCh := make(chan recvdVal, 1)
	lookupResCh := make(chan *lookupWithFollowupResult, 1)

	logger.Debugw("finding value", "key", internal.LoggableRecordKeyString(key))

	if rec, err := dht.getLocal(ctx, key); rec != nil && err == nil {
		select {
		case valCh <- recvdVal{
			Val:  rec.GetValue(),
			From: dht.self,
		}:
		case <-ctx.Done():
		}
	}

	go func() {
		defer close(valCh)
		defer close(lookupResCh)
		lookupRes, err := dht.runLookupWithFollowup(ctx, key,
			func(ctx context.Context, p peer.ID) ([]*peer.AddrInfo, error) {
				// For DHT query command
				routing.PublishQueryEvent(ctx, &routing.QueryEvent{
					Type: routing.SendingQuery,
					ID:   p,
				})

				rec, peers, err := dht.protoMessenger.GetValue(ctx, p, key)
				if err != nil {
					logger.Debugf("error getting closer peers: %s", err)
					return nil, err
				}

				// For DHT query command
				routing.PublishQueryEvent(ctx, &routing.QueryEvent{
					Type:      routing.PeerResponse,
					ID:        p,
					Responses: peers,
				})

				if rec == nil {
					return peers, nil
				}

				val := rec.GetValue()
				if val == nil {
					logger.Debug("received a nil record value")
					return peers, nil
				}
				if err := dht.Validator.Validate(key, val); err != nil {
					// make sure record is valid
					logger.Debugw("received invalid record (discarded)", "error", err)
					return peers, nil
				}

				// the record is present and valid, send it out for processing
				select {
				case valCh <- recvdVal{
					Val:  val,
					From: p,
				}:
				case <-ctx.Done():
					return nil, ctx.Err()
				}

				return peers, nil
			},
			func(*qpeerset.QueryPeerset) bool {
				select {
				case <-stopQuery:
					return true
				default:
					return false
				}
			},
		)

		if err != nil {
			return
		}
		lookupResCh <- lookupRes

		if ctx.Err() == nil {
			dht.refreshRTIfNoShortcut(kb.ConvertKey(key), lookupRes)
		}
	}()

	return valCh, lookupResCh
}

func (dht *IpfsDHT) refreshRTIfNoShortcut(key kb.ID, lookupRes *lookupWithFollowupResult) {
	if lookupRes.completed {
		// refresh the cpl for this key as the query was successful
		dht.routingTable.ResetCplRefreshedAtForID(key, time.Now())
	}
}

// Provider abstraction for indirect stores.
// Some DHTs store values directly, while an indirect store stores pointers to
// locations of the value, similarly to Coral and Mainline DHT.

// Provide makes this node announce that it can provide a value for the given key
func (dht *IpfsDHT) Provide(ctx context.Context, key cid.Cid, brdcst bool) (err error) {
	ctx, end := tracer.Provide(dhtName, ctx, key, brdcst)
	defer func() { end(err) }()

	if !dht.enableProviders {
		return routing.ErrNotSupported
	} else if !key.Defined() {
		return fmt.Errorf("invalid cid: undefined")
	}
	keyMH := key.Hash()
	logger.Debugw("providing", "cid", key, "mh", internal.LoggableProviderRecordBytes(keyMH))

	// add self locally
	dht.providerStore.AddProvider(ctx, keyMH, peer.AddrInfo{ID: dht.self})
	if !brdcst {
		return nil
	}

	if dht.enableOptProv {
		err := dht.optimisticProvide(ctx, keyMH)
		if errors.Is(err, netsize.ErrNotEnoughData) {
			logger.Debugln("not enough data for optimistic provide taking classic approach")
			return dht.classicProvide(ctx, keyMH)
		}
		return err
	}
	return dht.classicProvide(ctx, keyMH)
}

func (dht *IpfsDHT) classicProvide(ctx context.Context, keyMH multihash.Multihash) error {
	closerCtx := ctx
	if deadline, ok := ctx.Deadline(); ok {
		now := time.Now()
		timeout := deadline.Sub(now)

		if timeout < 0 {
			// timed out
			return context.DeadlineExceeded
		} else if timeout < 10*time.Second {
			// Reserve 10% for the final put.
			deadline = deadline.Add(-timeout / 10)
		} else {
			// Otherwise, reserve a second (we'll already be
			// connected so this should be fast).
			deadline = deadline.Add(-time.Second)
		}
		var cancel context.CancelFunc
		closerCtx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}

	var exceededDeadline bool
	peers, err := dht.GetClosestPeers(closerCtx, string(keyMH))
	switch err {
	case context.DeadlineExceeded:
		// If the _inner_ deadline has been exceeded but the _outer_
		// context is still fine, provide the value to the closest peers
		// we managed to find, even if they're not the _actual_ closest peers.
		if ctx.Err() != nil {
			return ctx.Err()
		}
		exceededDeadline = true
	case nil:
	default:
		return err
	}

	wg := sync.WaitGroup{}
	for _, p := range peers {
		wg.Add(1)
		go func(p peer.ID) {
			defer wg.Done()
			logger.Debugf("putProvider(%s, %s)", internal.LoggableProviderRecordBytes(keyMH), p)
			err := dht.protoMessenger.PutProviderAddrs(ctx, p, keyMH, peer.AddrInfo{
				ID:    dht.self,
				Addrs: dht.filterAddrs(dht.host.Addrs()),
			})
			if err != nil {
				logger.Debug(err)
			}
		}(p)
	}
	wg.Wait()
	if exceededDeadline {
		return context.DeadlineExceeded
	}
	return ctx.Err()
}

// FindProviders searches until the context expires.
func (dht *IpfsDHT) FindProviders(ctx context.Context, c cid.Cid) ([]peer.AddrInfo, error) {
	if !dht.enableProviders {
		return nil, routing.ErrNotSupported
	} else if !c.Defined() {
		return nil, fmt.Errorf("invalid cid: undefined")
	}

	var providers []peer.AddrInfo
	for p := range dht.FindProvidersAsync(ctx, c, dht.bucketSize) {
		providers = append(providers, p)
	}
	return providers, nil
}

// FindProvidersAsync is the same thing as FindProviders, but returns a channel.
// Peers will be returned on the channel as soon as they are found, even before
// the search query completes. If count is zero then the query will run until it
// completes. Note: not reading from the returned channel may block the query
// from progressing.
func (dht *IpfsDHT) FindProvidersAsync(ctx context.Context, key cid.Cid, count int) (ch <-chan peer.AddrInfo) {
	ctx, end := tracer.FindProvidersAsync(dhtName, ctx, key, count)
	defer func() { ch = end(ch, nil) }()

	if !dht.enableProviders || !key.Defined() {
		peerOut := make(chan peer.AddrInfo)
		close(peerOut)
		return peerOut
	}

	peerOut := make(chan peer.AddrInfo)

	keyMH := key.Hash()

	logger.Debugw("finding providers", "cid", key, "mh", internal.LoggableProviderRecordBytes(keyMH))
	go dht.findProvidersAsyncRoutine(ctx, keyMH, count, peerOut)
	return peerOut
}

func (dht *IpfsDHT) findProvidersAsyncRoutine(ctx context.Context, key multihash.Multihash, count int, peerOut chan peer.AddrInfo) {
	// use a span here because unlike tracer.FindProvidersAsync we know who told us about it and that intresting to log.
	ctx, span := internal.StartSpan(ctx, "IpfsDHT.FindProvidersAsyncRoutine")
	defer span.End()

	defer close(peerOut)

	findAll := count == 0

	ps := make(map[peer.ID]peer.AddrInfo)
	psLock := &sync.Mutex{}
	psTryAdd := func(p peer.AddrInfo) bool {
		psLock.Lock()
		defer psLock.Unlock()
		pi, ok := ps[p.ID]
		if (!ok || ((len(pi.Addrs) == 0) && len(p.Addrs) > 0)) && (len(ps) < count || findAll) {
			ps[p.ID] = p
			return true
		}
		return false
	}
	psSize := func() int {
		psLock.Lock()
		defer psLock.Unlock()
		return len(ps)
	}

	provs, err := dht.providerStore.GetProviders(ctx, key)
	if err != nil {
		return
	}
	for _, p := range provs {
		// NOTE: Assuming that this list of peers is unique
		if psTryAdd(p) {
			select {
			case peerOut <- p:
				// Add tracing event for finding a provider
				span.AddEvent("found provider", trace.WithAttributes(
					attribute.Stringer("peer", p.ID),
					attribute.Stringer("from", dht.self),
					attribute.Int("provider_addrs_count", len(p.Addrs)),
					attribute.Bool("found_in_provider_store", true),
				))
			case <-ctx.Done():
				return
			}
		}

		// If we have enough peers locally, don't bother with remote RPC
		// TODO: is this a DOS vector?
		if !findAll && len(ps) >= count {
			return
		}
	}

	lookupRes, err := dht.runLookupWithFollowup(ctx, string(key),
		func(ctx context.Context, p peer.ID) ([]*peer.AddrInfo, error) {

			// For DHT query command
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type: routing.SendingQuery,
				ID:   p,
			})

			provs, closest, err := dht.protoMessenger.GetProviders(ctx, p, key)
			if err != nil {
				return nil, err
			}

			logger.Debugf("%d provider entries", len(provs))

			// Add unique providers from request, up to 'count'
			for _, prov := range provs {
				dht.maybeAddAddrs(prov.ID, prov.Addrs, peerstore.TempAddrTTL)
				logger.Debugf("got provider: %s", prov)
				if psTryAdd(*prov) {
					logger.Debugf("using provider: %s", prov)
					select {
					case peerOut <- *prov:
						span.AddEvent("found provider", trace.WithAttributes(
							attribute.Stringer("peer", prov.ID),
							attribute.Stringer("from", p),
							attribute.Int("provider_addrs_count", len(prov.Addrs)),
						))
					case <-ctx.Done():
						logger.Debug("context timed out sending more providers")
						return nil, ctx.Err()
					}
				}
				if !findAll && psSize() >= count {
					logger.Debugf("got enough providers (%d/%d)", psSize(), count)
					return nil, nil
				}
			}

			// Give closer peers back to the query to be queried
			logger.Debugf("got closer peers: %d %s", len(closest), closest)

			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type:      routing.PeerResponse,
				ID:        p,
				Responses: closest,
			})

			return closest, nil
		},
		func(*qpeerset.QueryPeerset) bool {
			return !findAll && psSize() >= count
		},
	)

	if err == nil && ctx.Err() == nil {
		dht.refreshRTIfNoShortcut(kb.ConvertKey(string(key)), lookupRes)
	}
}

// FindPeer searches for a peer with given ID.
func (dht *IpfsDHT) FindPeer(ctx context.Context, id peer.ID) (pi peer.AddrInfo, err error) {
	ctx, end := tracer.FindPeer(dhtName, ctx, id)
	defer func() { end(pi, err) }()

	if err := id.Validate(); err != nil {
		return peer.AddrInfo{}, err
	}

	logger.Debugw("finding peer", "peer", id)

	// Check if were already connected to them
	if pi := dht.FindLocal(ctx, id); pi.ID != "" {
		return pi, nil
	}

	lookupRes, err := dht.runLookupWithFollowup(ctx, string(id),
		func(ctx context.Context, p peer.ID) ([]*peer.AddrInfo, error) {
			// For DHT query command
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type: routing.SendingQuery,
				ID:   p,
			})

			peers, err := dht.protoMessenger.GetClosestPeers(ctx, p, id)
			if err != nil {
				logger.Debugf("error getting closer peers: %s", err)
				return nil, err
			}

			// For DHT query command
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type:      routing.PeerResponse,
				ID:        p,
				Responses: peers,
			})

			return peers, err
		},
		func(*qpeerset.QueryPeerset) bool {
			return hasValidConnectedness(dht.host, id)
		},
	)

	if err != nil {
		return peer.AddrInfo{}, err
	}

	dialedPeerDuringQuery := false
	for i, p := range lookupRes.peers {
		if p == id {
			// Note: we consider PeerUnreachable to be a valid state because the peer may not support the DHT protocol
			// and therefore the peer would fail the query. The fact that a peer that is returned can be a non-DHT
			// server peer and is not identified as such is a bug.
			dialedPeerDuringQuery = (lookupRes.state[i] == qpeerset.PeerQueried || lookupRes.state[i] == qpeerset.PeerUnreachable || lookupRes.state[i] == qpeerset.PeerWaiting)
			break
		}
	}

	// Return peer information if we tried to dial the peer during the query or we are (or recently were) connected
	// to the peer.
	if dialedPeerDuringQuery || hasValidConnectedness(dht.host, id) {
		return dht.peerstore.PeerInfo(id), nil
	}

	return peer.AddrInfo{}, routing.ErrNotFound
}

// Used for experiments where we want to log data from the provide operation
func (dht *IpfsDHT) ProvideWithReturn(ctx context.Context, key cid.Cid, brdcst bool) (error, []peer.ID, []peer.ID, int) {
	var err error
	// dht.providerLk.Lock()         // This is just to prevent concurrent provides from annoying me for now. Will be removed later
	// defer dht.providerLk.Unlock() // This is just to prevent concurrent provides from annoying me for now. Will be removed later

	keyMH := key.Hash()
	// fmt.Println("Provide: cid", key, ", hash:", keyMH)

	if !dht.enableProviders {
		return routing.ErrNotSupported, make([]peer.ID, 0), make([]peer.ID, 0), 0
	} else if !key.Defined() {
		return fmt.Errorf("invalid cid: undefined"), make([]peer.ID, 0), make([]peer.ID, 0), 0
	}
	logger.Debugw("providing", "cid", key, "mh", internal.LoggableProviderRecordBytes(keyMH))

	// add self locally
	// dht.providerStore.AddProvider(ctx, keyMH, peer.AddrInfo{ID: dht.self})
	if !brdcst {
		return nil, make([]peer.ID, 0), make([]peer.ID, 0), 0
	}

	closerCtx := ctx
	if deadline, ok := ctx.Deadline(); ok {
		now := time.Now()
		timeout := deadline.Sub(now)

		if timeout < 0 {
			// timed out
			return context.DeadlineExceeded, make([]peer.ID, 0), make([]peer.ID, 0), 0
		} else if timeout < 10*time.Second {
			// Reserve 10% for the final put.
			deadline = deadline.Add(-timeout / 10)
		} else {
			// Otherwise, reserve a second (we'll already be
			// connected so this should be fast).
			deadline = deadline.Add(-time.Second)
		}
		var cancel context.CancelFunc
		closerCtx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}

	var exceededDeadline bool
	var peers []peer.ID
	var peersPutProvidersSuccess []peer.ID //peers for which putProviders was successful
	var netsizeErr error
	var netsize int32

	if dht.enableRegionProvide {
		netsize, netsizeErr = dht.NsEstimator.NetworkSize()
		if netsizeErr != nil {
			dht.GatherNetsizeData(ctx)
			netsize, netsizeErr = dht.NsEstimator.NetworkSize()
		}
	}
	var numLookups int
	if dht.enableRegionProvide && netsizeErr == nil {
		fmt.Println("Estimated network size as", netsize)
		// Calculate the expected maximum distance of the `provideRegionSize` number of closest peers.
		// Then calculate the minimum common prefix length of all peerids within that distance
		minCPL := int(math.Ceil(math.Log2(float64(netsize)/float64(dht.provideRegionSize)))) - 1
		fmt.Println("Providing cid", key, ", hash:", keyMH, "to all peers with CPL", minCPL)
		peers, numLookups, err = dht.GetPeersWithCPLGet(closerCtx, string(keyMH), minCPL)
		fmt.Println("Provide", key, "took", numLookups, "lookups.")
	} else {
		if netsizeErr != nil {
			fmt.Println("Defaulting to regular provide operation due to error in netsize estimation:", netsizeErr)
		}
		peers, err = dht.GetClosestPeers(closerCtx, string(keyMH))
	}

	switch err {
	case context.DeadlineExceeded:
		// If the _inner_ deadline has been exceeded but the _outer_
		// context is still fine, provide the value to the closest peers
		// we managed to find, even if they're not the _actual_ closest peers.
		if ctx.Err() != nil {
			return ctx.Err(), make([]peer.ID, 0), make([]peer.ID, 0), 0
		}
		exceededDeadline = true
	case nil:
	default:
		return err, make([]peer.ID, 0), make([]peer.ID, 0), 0
	}

	// fmt.Printf("Provide CID hash: %x\n", []byte(kb.ConvertKey(string(keyMH))))

	// fmt.Println("Sending provider record to", len(peers), "peers:")
	// for _, pid := range peers {
	// 	c := []byte(kb.ConvertKey(string(pid)))
	// 	fmt.Printf("%x\n", c)
	// }

	wg := sync.WaitGroup{}
	for _, p := range peers {
		wg.Add(1)
		go func(p peer.ID) {
			defer wg.Done()
			logger.Debugf("putProvider(%s, %s)", internal.LoggableProviderRecordBytes(keyMH), p)
			err := dht.protoMessenger.PutProvider(ctx, p, keyMH, dht.host)
			if err != nil {
				logger.Debug(err)
			} else {
				peersPutProvidersSuccess = append(peersPutProvidersSuccess, p)
			}
		}(p)
	}
	wg.Wait()
	if exceededDeadline {
		return context.DeadlineExceeded, make([]peer.ID, 0), make([]peer.ID, 0), 0
	}

	// TODO: Run region-based provide only if eclipse attack is detected
	//_, e := dht.EclipseDetection(ctx, keyMH, peers)
	//if e != nil {
	//    return err, make([]peer.ID, 0), make([]peer.ID, 0), 0
	//}

	return ctx.Err(), peers, peersPutProvidersSuccess, numLookups
}

// This flag controls whether the special provide option is invoked.
func (dht *IpfsDHT) EnableMitigation() {
	dht.enableRegionProvide = true
}

func (dht *IpfsDHT) DisableMitigation() {
	dht.enableRegionProvide = false
}

// This parameter controls how many DHT peers are sent the provider record, in case of a detected eclipse attack,
// The provider record is sent to all peers within the distance in which there are expected to be provideRegionSize peers
func (dht *IpfsDHT) SetProvideRegionSize(regionSize int) {
	dht.provideRegionSize = regionSize
}

func (dht *IpfsDHT) EclipseDetection(ctx context.Context, keyMH multihash.Multihash, peers []peer.ID) (bool, error) {
	if len(peers) < defaultEclipseDetectionK {
		return false, fmt.Errorf("not enough peers for eclipse detection: expected: %d, found: %d", defaultEclipseDetectionK, len(peers))
	}
	if len(peers) > defaultEclipseDetectionK {
		peers = peers[:defaultEclipseDetectionK]
	}

	// Eclipse attack detection here
	// fmt.Println("Testing cid hash", keyMH, "for eclipse attack...")
	if dht.Detector == nil {
		return false, fmt.Errorf("detector not initialized")
	}

	netSize, netSizeErr := dht.NsEstimator.NetworkSize()
	if netSizeErr != nil {
		err := dht.GatherNetsizeData(ctx)
		if err != nil {
			return false, netSizeErr
		}

		netSize, netSizeErr = dht.NsEstimator.NetworkSize()
		if netSizeErr != nil {
			return false, netSizeErr
		}
	}
	fmt.Println("Estimated network size as", netSize)

	dht.Detector.UpdateIdealDistFromNetsize(int(netSize))
	// fmt.Println("Estimated ideal distribution: ", idealDist)

	targetBytes := []byte(kb.ConvertKey(string(keyMH)))
	// fmt.Println("Eclipse attack detection for id hash:", keyMH)
	// fmt.Printf("CID in the DHT keyspace: %x \n", targetBytes)
	peeridsBytes := make([][]byte, len(peers))
	//fmt.Println("Number of peers obtained: ", len(peers))
	for i := range peeridsBytes {
		peeridsBytes[i] = []byte(kb.ConvertKey(string(peers[i])))
		// fmt.Printf("%s %x \n", peers[i], peeridsBytes[i])
		// fmt.Printf("%s \n", peers[i])
	}

	counts := dht.Detector.ComputePrefixLenCounts(targetBytes, peeridsBytes)
	kl := dht.Detector.ComputeKLFromCounts(counts)
	// fmt.Println("Counts:", counts)
	fmt.Println("KL divergence:", kl)
	result := dht.Detector.DetectFromKL(kl)
	// fmt.Println("Eclipse detection result:", result)
	var resultStr string
	if result {
		resultStr = "possible attack"
	} else {
		resultStr = "no attack"
	}
	fmt.Println("Eclipse attack detector says: ", resultStr, ", threshold =", dht.Detector.GetThreshold())
	// Eclipse attack detection code ends here
	return result, nil
}
