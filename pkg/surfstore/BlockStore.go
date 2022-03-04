package surfstore

import (
	context "context"
	"crypto/sha256"
	"encoding/hex"
	"sync"
)

type BlockStore struct {
	BlockMap map[string]*Block
	mu       sync.Mutex
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	//tem := blockHash.Hash
	bs.mu.Lock()
	defer bs.mu.Unlock()
	block, _ := bs.BlockMap[blockHash.Hash]
	return block, nil
}

// store block in the key-value store, indexed by hash value h
func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	h := sha256.New()
	h.Write(block.BlockData)
	hashBytes := h.Sum(nil)
	hashCode := hex.EncodeToString(hashBytes)
	bs.BlockMap[hashCode] = block
	//fmt.Println("serverblock", bs.BlockMap)
	return &Success{
		Flag: true,
	}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	blockHashesOutput := BlockHashes{Hashes: make([]string, 0)}
	for _, blockHash := range blockHashesIn.Hashes {
		if _, ok := bs.BlockMap[blockHash]; ok {
			blockHashesOutput.Hashes = append(blockHashesOutput.Hashes, blockHash)
		}
	}
	return &BlockHashes{
		Hashes: blockHashesOutput.Hashes,
	}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {

	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
