package surfstore

import (
	context "context"
	"errors"
	"sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	me             sync.Mutex
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	// Only retrieve the map
	m.me.Lock()
	defer m.me.Unlock()
	//fmt.Println("lllaaaaa", m.FileMetaMap)
	return &FileInfoMap{
		FileInfoMap: m.FileMetaMap,
	}, nil

}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	m.me.Lock()
	defer m.me.Unlock()
	filename := fileMetaData.Filename
	// File not exist in the metastore map
	if _, ok := m.FileMetaMap[filename]; ok {
		if fileMetaData.Version-m.FileMetaMap[filename].Version == 1 {
			m.FileMetaMap[filename] = fileMetaData
			//*latestVersion = fileMetaData.Version       // Update the lastest version as the new version.
			return &Version{
				Version: fileMetaData.Version, // Update the lastest version as the new version.
			}, nil
		} else {
			return &Version{
				Version: -1, // Update the lastest version as the new version.
			}, errors.New("New version number is NOT one greater than current version number")
		}
	} else {
		m.FileMetaMap[filename] = fileMetaData
		return &Version{
			Version: fileMetaData.Version, // Update the lastest version as the new version.
		}, nil
	}
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	m.me.Lock()
	defer m.me.Unlock()
	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
