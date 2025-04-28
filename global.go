package dht

import "github.com/libp2p/go-libp2p/core/peer"

var EclipsedCid string
var IsActive bool
var OtherSybils []peer.AddrInfo
var RandomProviders []peer.AddrInfo

func SetAttackConfiguration(eclipsedCid string, isActive bool, otherSybils []peer.AddrInfo, randomProviders []peer.AddrInfo) {
	EclipsedCid = eclipsedCid
	IsActive = isActive
	OtherSybils = otherSybils
	RandomProviders = randomProviders
}

// No need to bypass the RT diversity filter anymore, as the sybils can recommend each other without being interconnected
// var AllowedIps []string
// var AuthorizeAll = "0.0.0.0"
// var localGroup = "127.0.0.0"
// func SetGroupToBypassDiversityFilter(ip string) {
// 	AllowedIps = []string{localGroup, ip}
// }
