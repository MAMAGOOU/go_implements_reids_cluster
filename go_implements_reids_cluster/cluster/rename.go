package cluster

import (
	"go_implements_reids_cluster/interface/resp"
	"go_implements_reids_cluster/resp/reply"
)

// Rename rename k1 k2
func Rename(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	if len(cmdArgs) != 3 {
		return reply.MakeErrReply("ERR Wrong number args")
	}
	// k1
	src := string(cmdArgs[1])
	dest := string(cmdArgs[2])
	// address 192.168.0.0.1
	srcPeer := cluster.peerPicker.PickNode(src)
	destPeer := cluster.peerPicker.PickNode(dest)
	if srcPeer != destPeer {
		return reply.MakeErrReply("ERR rename must within on peer")
	}
	return cluster.relay(srcPeer, c, cmdArgs)
}
