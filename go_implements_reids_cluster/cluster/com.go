package cluster

import (
	"context"
	"errors"
	"go_implements_reids_cluster/interface/resp"
	"go_implements_reids_cluster/lib/utils"
	"go_implements_reids_cluster/resp/client"
	"go_implements_reids_cluster/resp/reply"
	"strconv"
)

// responsible for communication between nodes

// borrow
func (cluster *ClusterDatabase) getPeerClient(peer string) (*client.Client, error) {
	pool, ok := cluster.peerConnection[peer]
	if !ok {
		return nil, errors.New("connection not found")
	}
	object, err := pool.BorrowObject(context.Background())
	if err != nil {
		return nil, err
	}
	c, ok := object.(*client.Client)
	if !ok {
		return nil, errors.New("wrong type")
	}
	return c, err
}

// borrowed
func (cluster *ClusterDatabase) returnPeerClient(peer string, peerClient *client.Client) error {
	pool, ok := cluster.peerConnection[peer]
	if !ok {
		return errors.New("wrong type")
	}
	return pool.ReturnObject(context.Background(), peerClient)
}

func (cluster *ClusterDatabase) relay(peer string, c resp.Connection, args [][]byte) resp.Reply {
	if peer == cluster.self {
		return cluster.db.Exec(c, args)
	}
	peerClient, err := cluster.getPeerClient(peer)
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}
	defer func() {
		_ = cluster.returnPeerClient(peer, peerClient)
	}()
	peerClient.Send(utils.ToCmdLine("SELECT", strconv.Itoa(c.GetDBIndex())))
	return peerClient.Send(args)
}

// broadcast of instructions
func (cluster *ClusterDatabase) broadcast(c resp.Connection, args [][]byte) map[string]resp.Reply {
	results := make(map[string]resp.Reply)
	for _, node := range cluster.nodes {
		result := cluster.relay(node, c, args)
		results[node] = result
	}
	return results
}
