package cluster

import (
	"go_implements_reids_cluster/interface/resp"
	"go_implements_reids_cluster/resp/reply"
)

// Del del k1 k2 k3 k4
func Del(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	replies := cluster.broadcast(c, cmdArgs)
	var errReply reply.ErrorReply
	var deleted int64 = 0
	for _, r := range replies {
		if reply.IsErrorReply(r) {
			errReply = r.(reply.ErrorReply)
			break
		}
		intReply, ok := r.(*reply.IntReply)
		if !ok {
			errReply = reply.MakeErrReply("error")
		}
		deleted += intReply.Code
	}
	if errReply == nil {
		return reply.MakeIntReply(deleted)
	}
	return reply.MakeErrReply("error :" + errReply.Error())
}
