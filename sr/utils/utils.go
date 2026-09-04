package utils

// THIS FILE IS ONLY FOR DEBUGGING PURPOSES
// The functions available in this file should not be used in a real implementation.
// This file should be ignored and will be removed once the functionality is
// implemented in kad-dht.

import (
	"context"
	"fmt"
	"math/big"
	"time"

	gocid "github.com/ipfs/go-cid"
	"github.com/ipfs/kubo/core"
	"github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-kad-dht/qpeerset"
	"github.com/libp2p/go-libp2p-kad-dht/sr"
	kbucket "github.com/libp2p/go-libp2p-kbucket"
	kspace "github.com/libp2p/go-libp2p-kbucket/keyspace"
	"github.com/libp2p/go-libp2p/core/peer"
	mh "github.com/multiformats/go-multihash"
	"github.com/vicnetto/active-sybil-attack/logger"
	ipfspeer "github.com/vicnetto/active-sybil-attack/node/peer"
	"github.com/vicnetto/active-sybil-attack/utils/k-closest-to-file/interact"
)

var log = logger.InitializeLogger()

type Result struct {
	minCpl      int
	maxDistance *big.Int
}

func GetFarthestKByQueryWithAlreadyQueriedPeers(ctx context.Context, clientNode *core.IpfsNode, alreadyQueriedPeers *[]peer.ID) (*big.Int, error) {
	currentPeer := clientNode.DHT.WAN.GetValidPeerToQuery(ctx, *alreadyQueriedPeers)
	*alreadyQueriedPeers = append(*alreadyQueriedPeers, currentPeer)

	log.Info.Printf("  CID: %s", currentPeer.String())

	ctxTimeout, cancelTimeout := context.WithTimeout(ctx, 3*time.Minute)
	queryClosest, err := clientNode.DHT.WAN.QueryPeerForKClosestFromItself(ctxTimeout, currentPeer)
	if err != nil {
		log.Error.Println("Error while querying the peer:", err.Error())
		log.Error.Println("Retrying with another PID...")
		cancelTimeout()
		return big.NewInt(0), err
	}

	maxDistance := dht.GetFarthestDistance(currentPeer.String(), queryClosest, false)

	cancelTimeout()
	return maxDistance, nil
}

func GetFarthestKByDbWithAlreadyQueriedPeers(dbPath string, alreadyQueriedPeers *[]peer.ID) (*big.Int, error) {
	var cid gocid.Cid
	var peers []peer.ID
	var err error

	for {
		cid, peers, err = interact.GetRandomDHTLookup(dbPath)
		if err != nil {
			return big.NewInt(0), err
		}

		// Verify if the peer wasn't verified already
		for _, pid := range *alreadyQueriedPeers {
			if pid.String() == cid.String() {
				continue
			}
		}

		break
	}

	log.Info.Printf("  CID: %s", cid.String())

	peers, err = interact.GetClosestKFromContactedPeers(cid, peers)
	if err != nil {
		return big.NewInt(0), err
	}

	maxDistance := dht.GetFarthestDistance(cid.String(), peers, false)
	return maxDistance, err
}

func PerformRandomLookupReturningAllQueriedPeers(ctx context.Context, clientNode *core.IpfsNode) (gocid.Cid, []peer.ID) {
	var allPeersReceived *qpeerset.QueryPeerset
	var cidDecode gocid.Cid

	for {
		randomId, err := clientNode.DHT.WAN.RoutingTable().GenRandPeerID(0)
		if err != nil {
			log.Error.Println("Error while generating a random peer ID:", err)
		}

		cidDecode, err = gocid.Decode(randomId.String())
		if err != nil {
			log.Error.Println("Error while decoding random generated PID:", err)
		}

		ctxTimeout, ctxTimeoutCancel := context.WithTimeout(ctx, 120*time.Second)
		log.Info.Printf("  Getting closest peers to %s...", cidDecode.String())
		_, allPeersReceived, err = clientNode.DHT.WAN.GetAllClosestPeers(ctxTimeout, string(cidDecode.Hash()))
		if err != nil || allPeersReceived == nil {
			log.Error.Println("Error while asking the closest peers to the CID:", err)
			log.Error.Println("Retrying...")
			ctxTimeoutCancel()
			continue
		}

		ctxTimeoutCancel()
		break
	}

	return cidDecode, allPeersReceived.GetClosestInStates(qpeerset.PeerHeard, qpeerset.PeerWaiting, qpeerset.PeerQueried)
}

func CalculateAverageDistancePerPeerQuantity(ctx context.Context, maxPeerQuantity int) []sr.WelfordAverage {
	var averageMaxDistance []sr.WelfordAverage

	for i := 0; i < maxPeerQuantity; i++ {
		averageMaxDistance = append(averageMaxDistance, sr.WelfordAverage{})
	}

	for peerQuantity := 0; peerQuantity < maxPeerQuantity; peerQuantity++ {
		peerConfig := ipfspeer.ConfigForRandomNode(0)
		_, clientNode, err := ipfspeer.SpawnEphemeral(ctx, peerConfig)
		if err != nil {
			log.Error.Println("Error instantiating the clientNode:", err.Error())
			panic(err)
		}

		log.Info.Println("PID is UP:", clientNode.Identity.String())

		log.Info.Println("Sleep for 10 seconds before starting...")
		time.Sleep(10 * time.Second)
		fmt.Println()

		distanceAverage, err := clientNode.DHT.WAN.GetFarthestKAverageByQuery(ctx, peerQuantity+1)
		if err != nil {
			log.Error.Println("Error getting farthest k average:", err.Error())
			peerQuantity--
			continue
		}

		averageMaxDistance[peerQuantity] = distanceAverage

		fmt.Println()
	}

	return averageMaxDistance
}

type QuantityPerAverage map[sr.MeanType]int

func NewQuantityPerAverage() QuantityPerAverage {
	quantityPerAverage := QuantityPerAverage{}
	for meanType := sr.MeanType(0); meanType <= sr.LastMeanType; meanType++ {
		quantityPerAverage[meanType] = 0
	}
	return quantityPerAverage
}

func CountPeersPerAverage(cidDecode gocid.Cid, maxDistancePerPeerQuantity []sr.WelfordAverage,
	contactedPeers []peer.ID, peersPerDistance *map[sr.WelfordAverage]QuantityPerAverage) {

	targetCIDByte, _ := mh.FromB58String(cidDecode.String())
	targetCIDKey := kspace.XORKeySpace.Key(targetCIDByte)

	for _, currentPeer := range contactedPeers {
		peerByte, _ := mh.FromB58String(currentPeer.String())
		peerKey := kspace.XORKeySpace.Key(peerByte)
		distance := peerKey.Distance(targetCIDKey)
		cpl := kbucket.CommonPrefixLen(targetCIDKey.Bytes, peerKey.Bytes)

		for _, maxDistance := range maxDistancePerPeerQuantity {
			currentInfo := (*peersPerDistance)[maxDistance]

			if distance.Cmp(maxDistance.GetAverage(sr.Mean)) < 0 {
				currentInfo[sr.Mean] = currentInfo[sr.Mean] + 1
			}

			if distance.Cmp(maxDistance.GetAverage(sr.MeanStdDev)) < 0 {
				currentInfo[sr.MeanStdDev] = currentInfo[sr.MeanStdDev] + 1
			}

			if distance.Cmp(maxDistance.GetAverage(sr.WeightedMean)) < 0 {
				currentInfo[sr.WeightedMean] = currentInfo[sr.WeightedMean] + 1
			}

			if distance.Cmp(maxDistance.GetAverage(sr.WeightedMeanStdDev)) < 0 {
				currentInfo[sr.WeightedMeanStdDev] = currentInfo[sr.WeightedMeanStdDev] + 1
			}

			if cpl >= int(maxDistance.GetAverage(sr.CPL).Int64()) {
				currentInfo[sr.CPL] = currentInfo[sr.CPL] + 1
			}

			(*peersPerDistance)[maxDistance] = currentInfo
		}

		// for mt := sr.MeanType(0); mt <= Lastsr.MeanType; mt++ {
		// 	currentInfo := (*peersPerDistance)

		// 	toPrint += fmt.Sprintf(" %s: %d (%s);", mt.String(), peersPerDistance[distance][mt],
		// 		mitigation.ToSciNotation(distance.GetAverage(mt)))
		// }
	}

	// Minimum should be k=20, as in a standard scenario at least 20 will be sent.
	for _, maxDistance := range maxDistancePerPeerQuantity {
		currentInfo := (*peersPerDistance)[maxDistance]

		if currentInfo[sr.Mean] < 20 {
			currentInfo[sr.Mean] = 20
		}

		if currentInfo[sr.MeanStdDev] < 20 {
			currentInfo[sr.MeanStdDev] = 20
		}

		if currentInfo[sr.WeightedMean] < 20 {
			currentInfo[sr.WeightedMean] = 20
		}

		if currentInfo[sr.WeightedMeanStdDev] < 20 {
			currentInfo[sr.WeightedMeanStdDev] = 20
		}

		if currentInfo[sr.CPL] < 20 {
			currentInfo[sr.CPL] = 20
		}

		(*peersPerDistance)[maxDistance] = currentInfo
	}
}
