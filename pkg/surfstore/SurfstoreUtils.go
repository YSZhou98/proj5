package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

/*
Implement the logic for a client syncing with the server here.
*/
var SURF_CLIENT = "yunshu"

func ClientSync(client RPCClient) {
	//panic("todo")
	//var blockStoreAddr string
	//if err := client.GetBlockStoreAddr(&blockStoreAddr); err != nil {
	//	log.Fatal(err)
	//}
	if client.BaseDir[len(client.BaseDir)-1] == '/' {
		client.BaseDir = client.BaseDir[:len(client.BaseDir)-1]
	}
	var blockStoreAddr string
	if err := client.GetBlockStoreAddr(&blockStoreAddr); err != nil {
		log.Fatal(err)
	}

	indexPath := client.BaseDir + "/" + "index.txt"
	if _, indexFileErr := os.Stat(indexPath); os.IsNotExist(indexFileErr) {
		file, _ := os.Create(indexPath)
		defer file.Close()
	}
	//fmt.Println("address", indexPath)
	localIndexMap := make(map[string]FileMetaData)

	indexFile, _ := ioutil.ReadFile(indexPath)
	indexLines := strings.Split(string(indexFile), "\n")
	//var fileMetaData FileMetaData
	for _, line := range indexLines {
		if line == "" {
			continue
		}
		//fmt.Println(line)
		fileMetaData := encode(string(line))
		//fmt.Println(")))))))))))))", "jjjj", fileMetaData.Filename, "////struct////", fileMetaData)
		localIndexMap[fileMetaData.Filename] = fileMetaData
		//indexMap[fileMetaData.Filename] = i
	}
	//scan local dir
	root := client.BaseDir
	//fmt.Println("Index content", localIndexMap)

	//file walk of base direcotry
	//deletemap := make(map[string]bool)
	newfilemap := make(map[string]FileMetaData)
	dirmap := make(map[string]bool)
	//localdir := make(map[string]FileMetaData)
	_ = filepath.Walk(root,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if info.IsDir() { //skip current directory
				return nil
			}

			if path == indexPath { //ignore index.txt
				return nil
			}
			//fmt.Println(path)
			file, _ := os.Open(path)
			//data, _ := os.ReadFile(path)
			position := 0
			for i := 0; i < len(path); i++ {
				if path[i:i+1] == "/" {
					position = i
				}
			}

			subPath := path[position+1 : len(path)]
			//fmt.Println(subPath)
			dirmap[subPath] = true
			f, _ := os.Stat(path)
			numBlock := int(math.Ceil(float64(f.Size()) / float64(client.BlockSize)))
			// index map has the record
			if fileMetaData, ok := localIndexMap[subPath]; ok {
				var fileData FileMetaData
				changed, hashList := getHashList(file, fileMetaData, numBlock, client.BlockSize)
				//fmt.Println("________", subPath, changed)
				fileData.BlockHashList = hashList
				fileData.Version = fileMetaData.Version
				fileData.Filename = subPath
				if changed {
					//var a []string
					//a[0] = "0"
					// if fileData.BlockHashList[0] == a[0] {
					// 	//tem := true
					// 	deletemap[subPath] = true

					// }
					fileData.Version = fileMetaData.Version + 1
				}
				localIndexMap[subPath] = fileData
			} else {
				var newfile FileMetaData
				hashList := make([]string, numBlock)
				for i := 0; i < numBlock; i++ {
					// For each block, generate the hashList
					buf := make([]byte, client.BlockSize)
					n, e := file.Read(buf)
					if e != nil {
						log.Println("read error when getting hashList: ", e)
					}
					// Trim the buf
					buf = buf[:n]

					hash := sha256.New()
					hash.Write(buf)
					hashBytes := hash.Sum(nil)
					hashCode := hex.EncodeToString(hashBytes)
					hashList[i] = hashCode
				}

				newfile.Version = 1
				newfile.Filename = subPath
				newfile.BlockHashList = hashList
				newfilemap[subPath] = newfile
			}
			//fmt.Println("*******************")
			return nil
		})

	for k, v := range localIndexMap {
		//fmt.Println("^^^^^^^^^^^^^^^^^^")
		if _, ok := dirmap[k]; !ok {
			if len(v.BlockHashList) == 0 || (len(v.BlockHashList) > 0 && v.BlockHashList[0] != "0") {
				var deletefilebylocal FileMetaData
				deletefilebylocal.Version = localIndexMap[k].Version + 1
				deletefilebylocal.Filename = localIndexMap[k].Filename
				var a []string
				//fmt.Println("position0", k)
				a = append(a, "0")
				//fmt.Println("position0", k, a)
				deletefilebylocal.BlockHashList = a
				localIndexMap[k] = deletefilebylocal
			}
		}
	}

	// fmt.Println("LocalIndexpMap~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
	// for key, value := range localIndexMap {
	// 	fmt.Println("key", key)
	// 	fmt.Println(value.Filename)
	// 	fmt.Println(value.Version)
	// 	fmt.Println(value.BlockHashList)
	// 	fmt.Println("---------------------")
	// }
	// fmt.Println("deletemap~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
	// for key, value := range deletemap {
	// 	fmt.Println("key", key, value)
	// 	fmt.Println("---------------------")
	// }
	// fmt.Println("newfilemap~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
	// for key, value := range newfilemap {
	// 	fmt.Println("key", key)
	// 	fmt.Println(value.Filename)
	// 	fmt.Println(value.Version)
	// 	fmt.Println(value.BlockHashList)
	// 	fmt.Println("---------------------")
	// }
	writeindex(localIndexMap, newfilemap, indexPath)

	serverFileInfoMap := new(map[string]*FileMetaData) //new return a pointer
	//succ := new(bool)
	client.GetFileInfoMap(serverFileInfoMap) // rpc call, get index map from server
	//PrintMetaMap(*serverFileInfoMap)
	NewlocalIndexMap := make(map[string]FileMetaData)

	indexFile, _ = ioutil.ReadFile(indexPath)
	indexLines = strings.Split(string(indexFile), "\n")
	//var fileMetaData FileMetaData
	for _, line := range indexLines {
		if line == "" {
			continue
		}
		//fmt.Println(line)
		fileMetaData := encode(string(line))
		//fmt.Println(")))))))))))))", "jjjj", fileMetaData.Filename, "////struct////", fileMetaData)
		NewlocalIndexMap[fileMetaData.Filename] = fileMetaData
		//indexMap[fileMetaData.Filename] = i
	}
	//fmt.Println(NewlocalIndexMap)

	//Compare local index with remote server index
	for file_name, file_meta_data := range *serverFileInfoMap {
		//hashlist := file_meta_data.BlockHashList
		if _, ok := localIndexMap[file_name]; !ok {
			//if file does not exist in local index, server has new file
			//download file block from server
			hashlist := file_meta_data.BlockHashList
			var blockList []Block
			DownloadBlock(hashlist, &blockList, client, blockStoreAddr)
			// log.Println("blockList: ", blockList)
			JoinBlockAndDownloadFile(blockList, file_name, client)

		}
	}
	succ := new(bool)

	// client upload new file to server, if fail, download the file from server and update indexMap
	for name, _ := range NewlocalIndexMap {
		var latestVersion = new(int32)
		local_fmData := NewlocalIndexMap[name]
		err := client.UpdateFile(&local_fmData, latestVersion)
		if err != nil {
			// if error, meaning version mismatch. Download the file from server
			var file_meta_data = new(FileMetaData)
			file_meta_data = (*serverFileInfoMap)[name]
			//len(v.BlockHashList) == 0 || (len(v.BlockHashList) > 0 && v.BlockHashList[0] != "0")
			if file_meta_data != nil && (len(file_meta_data.BlockHashList) == 0 || (len(file_meta_data.BlockHashList) > 0 && file_meta_data.BlockHashList[0] != "0")) {
				hashlist := file_meta_data.BlockHashList
				var blockList []Block
				DownloadBlock(hashlist, &blockList, client, blockStoreAddr)
				JoinBlockAndDownloadFile(blockList, name, client)
				localIndexMap[name] = FileMetaData{Filename: name, Version: *latestVersion, BlockHashList: hashlist}
			}
		} else {
			// upload to block of file to server
			blockList := GetFileBlock(name, client)
			for _, block := range blockList {
				// log.Println("block data: ", block.BlockData)
				err = client.PutBlock(&block, blockStoreAddr, succ)
				//PrintError(err, "Put Block")
			}
		}
	}
	client.GetFileInfoMap(serverFileInfoMap)
	newmap := *serverFileInfoMap
	writeindexupdate(*serverFileInfoMap, indexPath)
	//PrintMetaMap(*serverFileInfoMap)
	for name, _ := range newmap {
		if _, ok := dirmap[name]; ok && len((*newmap[name]).BlockHashList) > 0 && (*newmap[name]).BlockHashList[0] == "0" {
			deletename := client.BaseDir + "/" + name
			err := os.Remove(deletename)

			if err != nil {
				// 删除失」
				fmt.Println("delete error")
			}

		}
	}

}

func GetFileBlock(name string, client RPCClient) (blockList []Block) {
	byteFile, err := ioutil.ReadFile(client.BaseDir + "/" + name) //this return a byte array
	if err != nil {
		log.Println("error reading file: ", name, err)
		return blockList
	}

	blockSize := client.BlockSize
	size := len(byteFile)/blockSize + 1
	block := make([]byte, 0, size)

	for i := 0; i < size; i++ {

		if i == size-1 { //if this is the last chunk, dont split the file
			block = byteFile[:len(byteFile)]
		} else {
			block, byteFile = byteFile[:blockSize], byteFile[blockSize:]
		}

		blockList = append(blockList, Block{BlockData: block, BlockSize: int32(len(block))})
	}

	return blockList

}

func DownloadBlock(hashList []string, blockList *[]Block, client RPCClient, blockStoreAddr string) {
	for _, blockHash := range hashList {
		var block Block
		_ = client.GetBlock(blockHash, blockStoreAddr, &block)
		// log.Println("downloaded block data: ", block.BlockData)
		//PrintError(err,"Get Block")
		(*blockList) = append(*blockList, block)
	}
}

func JoinBlockAndDownloadFile(blockList []Block, filename string, client RPCClient) {
	// join blocks of file
	var result []byte
	for _, block := range blockList {
		result = append(result, (block.BlockData)...)
	}

	//create file
	path := client.BaseDir + "/" + filename
	//fmt.Println(client.BaseDir)
	//fmt.Println(filename)
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		file, err := os.Create(path)
		if err != nil {
			log.Println("create index file error: ", err)
			return
		}
		defer file.Close()
	}
	err = ioutil.WriteFile(path, result, 0644)
}

func encode(line string) FileMetaData {
	var fileMetaData FileMetaData
	tokens := strings.Split(line, ",")
	fileMetaData.Filename = tokens[0]
	tem, _ := strconv.Atoi(tokens[1])
	fileMetaData.Version = int32(tem)
	hashListStr := tokens[2]
	hashListTokens := strings.Fields(hashListStr)
	var hashList []string
	for _, hashListToken := range hashListTokens {
		hashList = append(hashList, hashListToken)
	}
	fileMetaData.BlockHashList = hashList
	return fileMetaData
}

func Hash256(block []byte) (hash_code string) {
	h := sha256.New()
	h.Write(block)
	hash_code = hex.EncodeToString(h.Sum(nil))
	return hash_code
}

func getHashList(file *os.File, fileMetaData FileMetaData, numBlock int, blockSize int) (bool, []string) {
	hashList := make([]string, numBlock)
	var changed bool
	for i := 0; i < numBlock; i++ {
		// For each block, generate the hashList
		buf := make([]byte, blockSize)
		n, e := file.Read(buf)
		if e != nil {
			log.Println("read error when getting hashList: ", e)
		}
		// Trim the buf
		buf = buf[:n]

		hash := sha256.New()
		hash.Write(buf)
		hashBytes := hash.Sum(nil)
		hashCode := hex.EncodeToString(hashBytes)
		hashList[i] = hashCode
		if i >= len(fileMetaData.BlockHashList) || hashCode != fileMetaData.BlockHashList[i] {
			changed = true
		}
	}
	if numBlock != len(fileMetaData.BlockHashList) {
		changed = true
	}
	return changed, hashList
}

func writeindex(localIndexMap map[string]FileMetaData, newfilemap map[string]FileMetaData, indexPath string) {
	f, _ := os.OpenFile(indexPath, os.O_WRONLY|os.O_TRUNC, 0600)
	defer f.Close()

	//w, _ := f.WriteString("你的content")
	for _, v := range localIndexMap {
		data := v.Filename + "," + strconv.Itoa(int(v.Version)) + "," + strings.Join(v.BlockHashList, " ")
		fmt.Fprintln(f, data)
	}
	for _, v := range newfilemap {
		data := v.Filename + "," + strconv.Itoa(int(v.Version)) + "," + strings.Join(v.BlockHashList, " ")
		fmt.Fprintln(f, data)
	}

}

func writeindexupdate(SeverIndexMap map[string]*FileMetaData, indexPath string) {
	f, _ := os.OpenFile(indexPath, os.O_WRONLY|os.O_TRUNC, 0600)
	defer f.Close()

	for _, v := range SeverIndexMap {
		data := v.Filename + "," + strconv.Itoa(int(v.Version)) + "," + strings.Join(v.BlockHashList, " ")
		fmt.Fprintln(f, data)
	}

}
