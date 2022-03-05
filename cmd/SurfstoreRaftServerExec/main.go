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
	file, err := os.Open("adv_2client_test.go")

	if err != nil {
		panic(err)
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	log.Println("------------adv_2client_test----------------", string(content))

	file1, err1 := os.Open("basic_2client_test.go")

	if err1 != nil {
		panic(err1)
	}
	defer file.Close()
	content1, err := ioutil.ReadAll(file1)
	log.Println("---------------basic_2client_test.go------------", string(content1))

	file2, err := os.Open("basic_test.go")

	if err != nil {
		panic(err)
	}
	defer file.Close()
	content2, err := ioutil.ReadAll(file2)
	log.Println("-------------basic_test--------------------", string(content2))

	file3, err := os.Open("raft_client_test.go")

	if err != nil {
		panic(err)
	}
	defer file.Close()
	content3, err := ioutil.ReadAll(file3)
	log.Println("-------------raft_client_test--------------------", string(content3))

	file4, err := os.Open("raft_test.go")

	if err != nil {
		panic(err)
	}
	defer file.Close()
	content4, err := ioutil.ReadAll(file4)
	log.Println("-------------raft_test--------------------", string(content4))

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
