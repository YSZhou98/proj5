package surfstore

import (
	context "context"
	"fmt"
	"strings"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
	leaderindex    int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	// c := NewRaftSurfstoreClient(conn)
	c := NewBlockStoreClient(conn)
	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	//conn, err := grpc.Dial(surfClient.MetaStoreAddrs[0], grpc.WithInsecure())
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	tem, err := c.PutBlock(ctx, block)
	//*succ = tem.Flag

	if err != nil {
		conn.Close()
		return err
	}
	*succ = tem.Flag
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	InBlocks := BlockHashes{Hashes: blockHashesIn}
	tem, err := c.HasBlocks(ctx, &InBlocks)

	if err != nil {
		conn.Close()
		return err
	}
	*blockHashesOut = tem.Hashes
	// close the connection
	return conn.Close()

}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	// connect to the server
	conn, err := grpc.Dial(surfClient.MetaStoreAddrs[0], grpc.WithInsecure())
	//conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	// perform the call
	// perform the call
	// c := NewMetaStoreClient(conn)
	c := NewRaftSurfstoreClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	tem, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
	//*serverFileInfoMap = tem.FileInfoMap

	if err != nil {
		conn.Close()
		return err
	}
	*serverFileInfoMap = tem.FileInfoMap
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	var ERR_SERVER_CRASHED = fmt.Errorf("New version number is NOT one greater than current version number")
	//conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.Dial(surfClient.MetaStoreAddrs[0], grpc.WithInsecure())
	if err != nil {
		return err
	}
	// perform the call
	// perform the call
	// c := NewMetaStoreClient(conn)
	c := NewRaftSurfstoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	tem, err := c.UpdateFile(ctx, fileMetaData)
	if err != nil && strings.Contains(err.Error(), ERR_SERVER_CRASHED.Error()) {
		conn.Close()
		//panic("panic")
		return err
	}
	// if err == fmt.Errorf("not majority.") {
	// 	conn.Close()
	// 	panic("panic")
	// 	//return err
	// }
	if tem != nil {
		latestVersion = &tem.Version
	}
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
	// connect to the server
	//conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.Dial(surfClient.MetaStoreAddrs[0], grpc.WithInsecure())
	if err != nil {
		return err
	}
	// c := NewMetaStoreClient(conn)
	c := NewRaftSurfstoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	addr, err := c.GetBlockStoreAddr(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	*blockStoreAddr = addr.Addr

	// close the connection
	return conn.Close()
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	leader := -1
	//count := 0
	for idx, addr := range addrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			//fmt.Println("error", idx, err)
			panic("panic")
		}
		client := NewRaftSurfstoreClient(conn)
		state, err := client.GetInternalState(context.Background(), &emptypb.Empty{})
		if err != nil {
			panic("panic")
		}

		if state.IsLeader {
			leader = idx
		}

	}
	if leader == -1 {
		panic("panic")
	}
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
		leaderindex:    leader,
	}
}
