package surfstore

import (
	context "context"
	//"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RaftSurfstore struct {
	// TODO add any fields you need
	isLeader bool
	term     int64
	log      []*UpdateOperation
	me       sync.Mutex

	metaStore *MetaStore

	commitIndex    int64
	pendingCommits []chan bool

	lastApplied int64

	// Server Info
	ip       string
	ipList   []string
	serverId int64

	// Leader protection
	isLeaderMutex sync.RWMutex
	isLeaderCond  *sync.Cond

	rpcClients []RaftSurfstoreClient

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex sync.RWMutex
	notCrashedCond *sync.Cond

	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	// Only retrieve the map
	s.me.Lock()
	defer s.me.Unlock()
	//fmt.Println("lllaaaaa", m.FileMetaMap)
	return &FileInfoMap{
		FileInfoMap: s.metaStore.FileMetaMap,
	}, nil
	//return nil, nil
}

func (s *RaftSurfstore) GetBlockStoreAddr(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddr, error) {
	s.me.Lock()
	defer s.me.Unlock()
	return &BlockStoreAddr{Addr: s.metaStore.BlockStoreAddr}, nil
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	if s.isCrashed == true {
		return nil, fmt.Errorf("Server is crashed.")
	}
	if s.isLeader == false {
		return nil, fmt.Errorf("i am not a leader.")
	}
	op := UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}
	s.me.Lock()
	s.log = append(s.log, &op)
	s.me.Unlock()
	committed := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, committed)

	go s.attemptCommit()

	success := <-committed
	//fmt.Println("line 73", success)
	//time.Sleep(time.Millisecond * 2000)
	if success {
		return s.metaStore.UpdateFile(ctx, filemeta)
	}
	if !success {
		return nil, fmt.Errorf("not majority.")
	}

	return nil, nil
}

func (s *RaftSurfstore) attemptCommit() {
	targetIdx := s.commitIndex + 1
	commitChan := make(chan *AppendEntryOutput, len(s.ipList))
	for idx, _ := range s.ipList {
		if int64(idx) == s.serverId {
			continue
		}
		go s.commitEntry(int64(idx), targetIdx, commitChan)
	}

	commitCount := 1
	for {
		// TODO handle crashed nodes
		//fmt.Println("jjjj")
		commit := <-commitChan
		if commit != nil && commit.Success {
			commitCount++
		}
		if commitCount > len(s.ipList)/2 {
			fmt.Println(targetIdx)
			s.pendingCommits[targetIdx] <- true
			s.commitIndex = targetIdx
			break
		}
	}
}

func (s *RaftSurfstore) commitEntry(serverIdx, entryIdx int64, commitChan chan *AppendEntryOutput) {
	if s.isCrashed || !s.isLeader {
		return

	}
	for {
		addr := s.ipList[serverIdx]
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return
		}
		client := NewRaftSurfstoreClient(conn)
		//fmt.Println("enrty", entryIdx, s.log)
		// TODO create correct AppendEntryInput from s.nextIndex, etc
		var input AppendEntryInput
		if len(s.log) == 0 {
			//input := &AppendEntryInput{
			// Term:         s.term,
			// PrevLogTerm:  -1,
			// PrevLogIndex: -1,
			// Entries:      s.log[entryIdx : entryIdx+1],
			// LeaderCommit: s.commitIndex,
			input.Term = s.term
			input.PrevLogTerm = -1
			input.PrevLogIndex = -1
			input.Entries = make([]*UpdateOperation, 0)
			input.LeaderCommit = s.commitIndex

		} else {
			input.Term = s.term
			input.PrevLogTerm = -1
			input.PrevLogIndex = -1
			input.Entries = s.log[entryIdx : entryIdx+1]
			input.LeaderCommit = s.commitIndex

		}

		if s.isCrashed {
			break
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		output, _ := client.AppendEntries(ctx, &input)
		if output != nil && output.Success {
			commitChan <- output
			break
			//return
		}
		// TODO update state. s.nextIndex, etc
		//if output == nil || !output.Success {
		//commitChan <- fmt.Errorf("error")
		//commitChan <- output

		// TODO handle crashed/ non success cases
	}
}

//1. Reply false if term < currentTerm (§5.1)
//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
//matches prevLogTerm (§5.3)
//3. If an existing entry conflicts with a new one (same index but different
//terms), delete the existing entry and all that follow it (§5.3)
//4. Append any new entries not already in the log
//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
//of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {

	output := &AppendEntryOutput{
		Success:      false,
		MatchedIndex: -1,
	}

	//return output, nil
	if input.Term > s.term {
		s.isLeader = false
		s.term = input.Term
	}
	if input.PrevLogTerm == -3 {
		s.me.Lock()
		fmt.Println(s.serverId, input.Entries)
		s.log = input.Entries
		s.me.Unlock()
		output.Success = true
		if !s.isCrashed {
			s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))

			for s.lastApplied < s.commitIndex {
				s.lastApplied++
				entry := s.log[s.lastApplied]
				s.metaStore.UpdateFile(ctx, entry.FileMetaData)
			}
		}

		return output, nil
	} else {
		if s.isCrashed {
			return &AppendEntryOutput{Success: false, MatchedIndex: -1}, nil

		}
		//1. Reply false if term < currentTerm (§5.1)
		//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
		//matches prevLogTerm (§5.3)
		//3. If an existing entry conflicts with a new one (same index but different
		//terms), delete the existing entry and all that follow it (§5.3)
		//4. Append any new entries not already in the log
		s.me.Lock()
		//fmt.Println("line 204", s.serverId)
		//fmt.Println(input.Entries)
		s.log = append(s.log, input.Entries...)
		s.me.Unlock()

		//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
		//of last new entry)
		// TODO only do this if leaderCommit > commitIndex
		if input.LeaderCommit > s.commitIndex {
			s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))
			count := 0
			for idx, addr := range s.ipList {
				if int64(idx) == s.serverId {
					continue
				}

				_, err := grpc.Dial(addr, grpc.WithInsecure())
				if err != nil {
					return nil, nil
				} else {
					count += 1
				}
			}
			if count >= len(s.ipList)/2 {

				for s.lastApplied < s.commitIndex {
					s.lastApplied++
					entry := s.log[s.lastApplied]
					s.me.Lock()
					s.metaStore.UpdateFile(ctx, entry.FileMetaData)
					s.me.Unlock()
				}

				//output.Success = true

				//return &AppendEntryOutput{Success: true, MatchedIndex: -1}, nil

				//} else {
				//	return &AppendEntryOutput{Success: false, MatchedIndex: -1}, nil

			}
		}
		return &AppendEntryOutput{Success: true, MatchedIndex: -1}, nil
	}
}

// This should set the leader status and any related variables as if the node has just won an election
func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.term++
	s.isLeaderMutex.Lock()
	s.isLeader = true
	s.isLeaderMutex.Unlock()
	return &Success{Flag: true}, nil
}

// Send a 'Heartbeat" (AppendEntries with no log entries) to the other servers
// Only leaders send heartbeats, if the node is not the leader you can return Success = false
func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	if !s.isLeader {
		return &Success{Flag: false}, fmt.Errorf("Server is not the leader")
	}

	count := 0

	for idx, addr := range s.ipList {
		if int64(idx) == s.serverId {
			continue
		}

		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return nil, nil
		}
		client := NewRaftSurfstoreClient(conn)

		// TODO create correct AppendEntryInput from s.nextIndex, etc
		input := &AppendEntryInput{
			Term:         s.term,
			PrevLogTerm:  -1,
			PrevLogIndex: -1,
			// TODO figure out which entries to send
			Entries: make([]*UpdateOperation, 0),
			//Entries: make([]*UpdateOperation, 0),
			//Entries:      s.log,
			LeaderCommit: s.commitIndex,
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		//sta, _ := client.GetInternalState(ctx, &emptypb.Empty{})
		//sta.Log
		//if sta != nil && s != nil && ((len(sta.Log) > 0 && len(s.log) > 0 && sta.Log[len(sta.Log)-1] != s.log[len(s.log)-1]) || (len(sta.Log) == 0)) {
		//	input.Entries = s.log
		if s != nil {
			input.Entries = s.log
		}
		input.PrevLogTerm = -3
		//}
		output, err := client.AppendEntries(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("Server is crashed.")
		}
		if output.Success {
			count += 1
			//return &Success{Flag: true}, nil
			// server is alive
		}
	}
	if count >= len(s.ipList)/2 {
		return &Success{Flag: true}, nil
	}
	return &Success{Flag: false}, fmt.Errorf("Majority crashed.")
	//return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	//s.log = make([]*UpdateOperation, 0)
	s.isLeaderMutex.Lock()
	s.isLeader = false
	s.isLeaderMutex.Unlock()
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.notCrashedCond.Broadcast()
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) IsCrashed(ctx context.Context, _ *emptypb.Empty) (*CrashedState, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	//s.notCrashedCond.Broadcast()
	s.isCrashedMutex.Unlock()
	s.isLeaderMutex.Lock()
	s.isLeader = false
	s.isLeaderMutex.Unlock()
	return &CrashedState{IsCrashed: s.isCrashed}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	//fmt.Println("hhh", s.isCrashed)
	return &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
		//crash:    s.isCrashed,
	}, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
