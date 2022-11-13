package cluster

import (
	"context"
	pool "github.com/jolestar/go-commons-pool/v2"
	"go_implements_reids_cluster/config"
	database2 "go_implements_reids_cluster/database"
	"go_implements_reids_cluster/interface/database"
	"go_implements_reids_cluster/interface/resp"
	"go_implements_reids_cluster/lib/consistenhash"
	"go_implements_reids_cluster/lib/logger"
	"go_implements_reids_cluster/resp/reply"
	"strings"
)

type ClusterDatabase struct {
	self       string
	nodes      []string
	peerPicker *consistenhash.NodeMap
	//use map to store multiple node values
	peerConnection map[string]*pool.ObjectPool
	db             database.Database
}

// MakeClusterDatabase initializes a connection between nodes
func MakeClusterDatabase() *ClusterDatabase {
	cluster := &ClusterDatabase{
		self:           config.Properties.Self,
		db:             database2.NewStandaloneDatabase(),
		peerPicker:     consistenhash.NewNodeMap(nil),
		peerConnection: make(map[string]*pool.ObjectPool),
	}
	nodes := make([]string, 0, len(config.Properties.Peers)+1)
	for _, peer := range config.Properties.Peers {
		nodes = append(nodes, peer)
	}
	nodes = append(nodes, config.Properties.Self)
	cluster.peerPicker.AddNode(nodes...)
	ctx := context.Background()
	for _, peer := range config.Properties.Peers {
		cluster.peerConnection[peer] = pool.NewObjectPoolWithDefaultConfig(ctx, &connectionFactory{
			Peer: peer,
		})
	}
	cluster.nodes = nodes
	return cluster
}

type CmdFunc func(cluster *ClusterDatabase, c resp.Connection, cmdArgs [][]byte) resp.Reply

var router = makeRouter()

// Exec cluster layer execution
func (cluster *ClusterDatabase) Exec(client resp.Connection, args [][]byte) (result resp.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
			result = &reply.UnknownErrReply{}
		}
	}()
	cmdName := strings.ToLower(string(args[0]))
	cmdFunc, ok := router[cmdName]
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmdName + "', or not supported in cluster mode")
	}
	result = cmdFunc(cluster, client, args)
	return
}

func (cluster *ClusterDatabase) AfterClientClose(c resp.Connection) {
	cluster.db.AfterClientClose(c)
}

func (cluster *ClusterDatabase) Close() {
	cluster.db.Close()
}
