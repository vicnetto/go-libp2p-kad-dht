package dht

import (
	"context"
	"fmt"
	gocid "github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-kad-dht/sr"
	kbucket "github.com/libp2p/go-libp2p-kbucket"
	kspace "github.com/libp2p/go-libp2p-kbucket/keyspace"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-base32"
	mh "github.com/multiformats/go-multihash"
	loggervicnetto "github.com/vicnetto/active-sybil-attack/logger"
	"math/big"
	"math/rand"
	"os"
	"slices"
	"time"
)

const KeySpace = 255

type EstimationMethod int

const (
	Lookup EstimationMethod = iota
	Query
)

var log = loggervicnetto.InitializeLogger()

func removePeersFromList(base []peer.ID, remove []peer.ID) []peer.ID {
	for _, r := range remove {
		if index := slices.Index(base, r); index != -1 {
			base = append(base[:index], base[index+1:]...)
		}
	}

	return base
}

// GetFarthestDistance returns the CPL and distance of the farthest k node of the closest list.
func GetFarthestDistance(target string, closest []peer.ID, print bool) *big.Int {
	targetCIDByte, _ := mh.FromB58String(target)
	targetCIDKey := kspace.XORKeySpace.Key(targetCIDByte)

	var maxDistance *big.Int
	if print {
		log.Info.Printf("Closest from %s: [", target)
	}
	for i, node := range closest {
		peerMultiHash, _ := mh.FromB58String(node.String())
		peerKey := kspace.XORKeySpace.Key(peerMultiHash)

		cpl := kbucket.CommonPrefixLen(targetCIDKey.Bytes, peerKey.Bytes)

		distance := peerKey.Distance(targetCIDKey)
		if i == 0 || distance.Cmp(maxDistance) > 0 {
			maxDistance = distance
		}

		if print {
			log.Info.Printf(" {%d, (CPL: %d) (Distance: %s) %s (%s)}", i+1, cpl, ToSciNotation(distance), node.String(), base32.RawStdEncoding.EncodeToString([]byte(node)))
		}
	}
	if print {
		log.Info.Println("]")
		fmt.Println()
	}

	return maxDistance
}

// ToSciNotation returns a string with the scientific notation of the big.Int.
func ToSciNotation(x *big.Int) string {
	if x.Cmp(big.NewInt(0)) == 0 {
		return "0"
	}

	if x.Cmp(big.NewInt(int64(KeySpace))) < 0 {
		return x.String()
	}

	// Get the absolute value of the big.Int
	absValue := new(big.Int).Abs(x)

	// Convert to float64 for calculation of scientific notation
	floatValue := new(big.Float).SetInt(absValue)
	mantissa := new(big.Float)
	exponent := float64(0)

	// Normalize the float to [1,10) range
	for floatValue.Cmp(big.NewFloat(10)) >= 0 {
		floatValue.Quo(floatValue, big.NewFloat(10))
		exponent++
	}

	for floatValue.Cmp(big.NewFloat(1)) < 0 {
		floatValue.Mul(floatValue, big.NewFloat(10))
		exponent--
	}

	// Format mantissa and exponent as a string
	mantissa.Float64()
	sign := ""
	if x.Sign() < 0 {
		sign = "-"
	}

	return fmt.Sprintf("%s%.3fe%d", sign, floatValue, int(exponent))
}

func (dht *IpfsDHT) QueryPeerForKClosestFromItself(ctx context.Context, pid peer.ID) ([]peer.ID, error) {
	// After asking directly from the peer, we need to know its addresses. As the peer is already in the RT, probably
	// we already know this information, but its probably better to be sure.
	if _, err := dht.FindPeer(ctx, pid); err != nil {
		return nil, err
	}

	// Create new routing table to allow generation of CIDs within an CPL.
	pidMultiHash, _ := mh.FromB58String(pid.String())
	rt, err := kbucket.NewRoutingTable(20, kbucket.ConvertKey(string(pidMultiHash)), time.Minute, dht.peerstore, time.Minute, nil)
	if err != nil {
		return nil, err
	}

	randomId, err := rt.GenRandPeerID(15)
	// log.Info.Println("Random PID)", randomId)

	// Ask directly the peer for the closest nodes he knows for the random generated peer in the closest CPL as possible.
	closest, err := dht.protoMessenger.GetClosestPeers(ctx, pid, randomId)
	if err != nil {
		return nil, err
	}

	// Remove garbage addresses, returning only the peer.ID
	var closestPeerId []peer.ID
	for _, peerInfo := range closest {
		closestPeerId = append(closestPeerId, peerInfo.ID)
	}

	return closestPeerId, nil
}

func (dht *IpfsDHT) GetValidPeerToQuery(ctx context.Context, alreadyQueriedPeers []peer.ID) peer.ID {
	var nextPeerToQuery peer.ID

	for {
		peersInRT := dht.RoutingTable().ListPeers()
		validPeersInRT := removePeersFromList(peersInRT, alreadyQueriedPeers)

		// In case there are no more valid peers to query, perform a random DHT query.
		if len(validPeersInRT) == 0 {
			log.Info.Printf("Already asked for all the nodes in the RT. Performing random DHT request...")
			time.Sleep(1 * time.Second)

			ctxTimeout, cancelCtxTimeout := context.WithTimeout(ctx, 10*time.Second)
			id, err := dht.RoutingTable().GenRandPeerID(0)
			if err != nil {
				log.Error.Println("Error generating random peer:", err)
				panic(err)
			}

			_, err = dht.GetClosestPeers(ctxTimeout, id.String())
			if err != nil && !os.IsTimeout(err) {
				log.Error.Println("Error filling the RT using a GetClosestPeers:", err)
				cancelCtxTimeout()
				continue
			}

			cancelCtxTimeout()
			continue
		}

		if len(validPeersInRT) == 0 {
			continue
		}

		// In case there are valid peers available, get a random one.
		randomPosition := rand.Intn(len(validPeersInRT))
		nextPeerToQuery = validPeersInRT[randomPosition]
		if slices.Contains(alreadyQueriedPeers, nextPeerToQuery) {
			continue
		}

		break
	}

	return nextPeerToQuery
}

func (dht *IpfsDHT) GetFarthestKAverageByQuery(ctx context.Context, nodesToContact int) (sr.WelfordAverage, error) {
	return dht.GetFarthestKAverage(ctx, nodesToContact, nil, Query)
}

func (dht *IpfsDHT) GetFarthestKAverageByLookup(ctx context.Context, nodesToContact int, initialDistance *sr.WelfordAverage) (sr.WelfordAverage, error) {
	return dht.GetFarthestKAverage(ctx, nodesToContact, initialDistance, Lookup)
}

func (dht *IpfsDHT) GetFarthestKAverage(ctx context.Context, nodesToContact int, initialDistance *sr.WelfordAverage, em EstimationMethod) (sr.WelfordAverage, error) {
	alreadyQueriedPeers := &[]peer.ID{}

	// var minCplResponse []int
	// var maxDistanceResponse []*big.Int
	var maxDistanceResponseStd = sr.NewWelfordMovingAverage()
	// minCplAverage := float64(0)

	if initialDistance != nil {
		// log.Info.Println("Starting average with previous value calculated:", ToSciNotation((*initialDistance).GetAverage(sr.MeanStdDev)))
		maxDistanceResponseStd = sr.NewWelfordMovingAverageFromMean(*initialDistance)
	}

	// log.Info.Printf("Obtaining minCpl and maxDistance by contacting %d peers...", nodesToContact)

	for peersContacted := 0; peersContacted < nodesToContact; peersContacted++ {
		var maxDistance *big.Int
		var err error

		switch em {
		case Lookup:
			// log.Info.Printf("%d) Getting random DHT lookup from the DB...", peersContacted+1)
			maxDistance, err = dht.GetFarthestKByLookup(ctx)
			if err != nil {
				log.Info.Println("Error during lookup for the k distance:", err)
				peersContacted--
				continue
			}
		case Query:
			// Case contrary, ask a random peer directly.
			// log.Info.Printf("%d) Querying random peer for their closest peers...", peersContacted+1)
			currentPeer := dht.GetValidPeerToQuery(ctx, *alreadyQueriedPeers)
			*alreadyQueriedPeers = append(*alreadyQueriedPeers, currentPeer)
			maxDistance, err = dht.GetFarthestKByQuery(ctx, currentPeer)
			if err != nil {
				log.Info.Println("Error querying peer for the k distance:", err)
				peersContacted--
				continue
			}
		}

		maxDistanceResponseStd.Add(maxDistance)

		// log.Info.Printf("  Max Distance: %s (%s)", ToSciNotation(maxDistance), maxDistance)
		// log.Info.Printf("  Average:")
		// log.Info.Printf("    Min CPL: %d", maxDistanceResponseStd.GetAverage(sr.CPL))
		// log.Info.Printf("    Mean, STD, M + STD: %s, %s, %s",
		// 	ToSciNotation(maxDistanceResponseStd.GetAverage(sr.Mean)),
		// 	ToSciNotation(maxDistanceResponseStd.GetStdDevAsInt(sr.Mean)),
		// 	ToSciNotation(maxDistanceResponseStd.GetAverage(sr.MeanStdDev)))
		// log.Info.Printf("    Weighted Mean, STD, WM + STD : %s, %s, %s",
		// 	ToSciNotation(maxDistanceResponseStd.GetAverage(sr.WeightedMean)),
		// 	ToSciNotation(maxDistanceResponseStd.GetStdDevAsInt(sr.WeightedMean)),
		// 	ToSciNotation(maxDistanceResponseStd.GetAverage(sr.WeightedMeanStdDev)))
		// log.Info.Printf("    Error Squared : %s",
		// 	ToSciNotation(maxDistanceResponseStd.GetErrorSquaredAverage()))
	}

	return *maxDistanceResponseStd, nil
}

func (dht *IpfsDHT) GetFarthestKByQuery(ctx context.Context, peer peer.ID) (*big.Int, error) {
	// currentPeer := dht.getValidPeerToQuery(ctx, *alreadyQueriedPeers)
	// *alreadyQueriedPeers = append(*alreadyQueriedPeers, currentPeer)

	// log.Info.Printf("  CID: %s", peer.String())

	ctxTimeout, cancelTimeout := context.WithTimeout(ctx, 10*time.Second)
	queryClosest, err := dht.QueryPeerForKClosestFromItself(ctxTimeout, peer)
	if err != nil {
		cancelTimeout()
		return big.NewInt(0), fmt.Errorf("error while querying the peer: %s", err.Error())
	}

	maxDistance := GetFarthestDistance(peer.String(), queryClosest, false)

	cancelTimeout()
	return maxDistance, nil
}

func (dht *IpfsDHT) GetFarthestKByLookup(ctx context.Context) (*big.Int, error) {
	var cid gocid.Cid
	var peers []peer.ID

	for {
		// Generate random peer using the Kubo function
		randomPid, err := dht.RoutingTable().GenRandPeerID(0)
		if err != nil {
			fmt.Println(err)
			continue
		}

		cid, err = gocid.Decode(randomPid.String())
		if err != nil {
			fmt.Println(err)
			continue
		}

		// log.Info.Printf("Getting closest peers to %s...", cid.String())
		timeoutCtx, cancelTimeoutCtx := context.WithTimeout(ctx, 30*time.Second)

		// Get the closest peers to verify the CPL of each one
		peers, err = dht.GetClosestPeers(timeoutCtx, string(cid.Hash()))
		if err != nil {
			log.Error.Printf("  %s", err)
			cancelTimeoutCtx()
			continue
		}

		cancelTimeoutCtx()
		break
	}

	maxDistance := GetFarthestDistance(cid.String(), peers, false)
	return maxDistance, nil
}
