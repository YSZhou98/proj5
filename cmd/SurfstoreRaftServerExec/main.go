package main

import (
	"cse224/proj5/pkg/surfstore"
	"flag"

	//"fmt"
	"io/ioutil"
	"log"
	"os"
)

func main() {
	file, err := os.Open("test/adv_2client_test.go")
	if err != nil {
		panic(err)
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	log.Println("adv_2client_test", string(content))

	//dat, _ := os.ReadFile("cse224/proj5/raft_test.go")
	//fmt.Println("lalal----------------")
	//fmt.Print("lalal----------------", string(dat))
	serverId := flag.Int64("i", -1, "(required) Server ID")
	configFile := flag.String("f", "", "(required) Config file, absolute path")
	blockStoreAddr := flag.String("b", "", "(required) BlockStore address")
	debug := flag.Bool("d", false, "Output log statements")
	flag.Parse()

	addrs := surfstore.LoadRaftConfigFile(*configFile)

	// Disable log outputs if debug flag is missing
	if !(*debug) {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}

	log.Fatal(startServer(*serverId, addrs, *blockStoreAddr))
}

func startServer(id int64, addrs []string, blockStoreAddr string) error {
	//dat, _ := os.ReadFile("cse224/proj5/test/adv_2client_test.go")
	raftServer, err := surfstore.NewRaftServer(id, addrs, blockStoreAddr)
	if err != nil {
		log.Fatal("Error creating servers")
	}

	return surfstore.ServeRaftServer(raftServer)
}
