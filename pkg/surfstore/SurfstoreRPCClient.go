package surfstore

import (
	context "context"
	"fmt"
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
	i := 0
	for i < len(surfClient.MetaStoreAddrs) {
		conn, err := grpc.Dial(surfClient.MetaStoreAddrs[i], grpc.WithInsecure())
		if err != nil {
			return err
		}
		// perform the call
		// perform the call
		// c := NewMetaStoreClient(conn)
		c := NewRaftSurfstoreClient(conn)
		state, err := c.GetInternalState(context.Background(), &emptypb.Empty{})
		if err != nil {
			continue
		}
		if state.IsLeader {
			fmt.Println("the server", i)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			tem, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
			if err != nil {
				panic("panic")
				//return err
			}
			*serverFileInfoMap = tem.FileInfoMap
			return conn.Close()
		}
		i += 1
	}
	return nil
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	//conn, err := grpc.Dial(surfClient.MetaStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	//surfClient.
	i := 0
	fmt.Println("=======Update file==========", len(surfClient.MetaStoreAddrs))
	for i < len(surfClient.MetaStoreAddrs) {
		conn, err := grpc.Dial(surfClient.MetaStoreAddrs[i], grpc.WithInsecure())
		if err != nil {
			return err
		}
		// perform the call
		// perform the call
		// c := NewMetaStoreClient(conn)
		c := NewRaftSurfstoreClient(conn)
		state, err := c.GetInternalState(context.Background(), &emptypb.Empty{})
		if err != nil {
			continue
		}
		if state.IsLeader {
			fmt.Println("the server", i)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			tem, err := c.UpdateFile(ctx, fileMetaData)
			if err != nil {
				conn.Close()
				return err
			}
			latestVersion = &tem.Version
			// close the connection
			return conn.Close()

		}
		i += 1
	}
	return fmt.Errorf("No server")
}

func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
	// connect to the server
	i := 0
	for i < len(surfClient.MetaStoreAddrs) {
		conn, err := grpc.Dial(surfClient.MetaStoreAddrs[i], grpc.WithInsecure())
		if err != nil {
			return err
		}
		// perform the call
		// perform the call
		// c := NewMetaStoreClient(conn)
		c := NewRaftSurfstoreClient(conn)
		state, err := c.GetInternalState(context.Background(), &emptypb.Empty{})
		if err != nil {
			continue
		}
		if state.IsLeader {
			fmt.Println("the server", i)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			addr, err := c.GetBlockStoreAddr(ctx, &emptypb.Empty{})
			if err != nil {
				panic("panic")
				//return err
			}
			*blockStoreAddr = addr.Addr
			return conn.Close()
		}
		i += 1
	}
	return nil
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
		leaderindex:    -1,
	}
}
