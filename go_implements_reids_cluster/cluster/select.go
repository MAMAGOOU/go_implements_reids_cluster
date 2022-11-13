package cluster

import "go_implements_reids_cluster/interface/resp"

func execSelect(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply {
	// ping cannot be forwarded
	return cluster.db.Exec(c, cmdArgs)
}
