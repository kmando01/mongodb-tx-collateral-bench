package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const (
	mongoURI     = "mongodb://localhost:27017/?replicaSet=rs0"
	dbName       = "collateral_bench"
	hotCollName  = "reward_history"  // Group A — long-running transaction target
	coldCollName = "product_catalog" // Group B — unrelated collection
	coldDocCount = 1000
	opsPerWorker = 30 // Group B reads per worker
)

// errAbortOnPurpose causes WithTransaction to abort without retry.
// MongoDB retries only on TransientTransactionError / UnknownTransactionCommitResult.
var errAbortOnPurpose = fmt.Errorf("abort_on_purpose")

// --- serverStatus snapshot ---

type TxSnapshot struct {
	CurrentOpen     int64
	CurrentActive   int64
	CurrentInactive int64
	TotalAborted    int64
	TotalCommitted  int64
	ReadTicketsOut  int64
	WriteTicketsOut int64
}

func takeSnapshot(cli *mongo.Client) TxSnapshot {
	var rawBson bson.M
	_ = cli.Database("admin").RunCommand(context.Background(),
		bson.D{{Key: "serverStatus", Value: 1}}).Decode(&rawBson)
	b, _ := bson.MarshalExtJSON(rawBson, true, false)
	var raw map[string]interface{}
	_ = json.Unmarshal(b, &raw)

	getInt := func(m map[string]interface{}, keys ...string) int64 {
		cur := interface{}(m)
		for _, k := range keys {
			if cur == nil {
				return 0
			}
			if mm, ok := cur.(map[string]interface{}); ok {
				cur = mm[k]
			} else {
				return 0
			}
		}
		switch v := cur.(type) {
		case float64:
			return int64(v)
		case map[string]interface{}:
			for _, field := range []string{"$numberLong", "$numberInt"} {
				if s, ok := v[field].(string); ok {
					var n int64
					fmt.Sscanf(s, "%d", &n)
					return n
				}
			}
		}
		return 0
	}

	return TxSnapshot{
		CurrentOpen:     getInt(raw, "transactions", "currentOpen"),
		CurrentActive:   getInt(raw, "transactions", "currentActive"),
		CurrentInactive: getInt(raw, "transactions", "currentInactive"),
		TotalAborted:    getInt(raw, "transactions", "totalAborted"),
		TotalCommitted:  getInt(raw, "transactions", "totalCommitted"),
		ReadTicketsOut:  getInt(raw, "queues", "execution", "read", "out"),
		WriteTicketsOut: getInt(raw, "queues", "execution", "write", "out"),
	}
}

// countCurrentOpTx runs db.currentOp({"transaction":{$exists:true}}) and returns open tx count.
func countCurrentOpTx(cli *mongo.Client) int64 {
	var result bson.M
	err := cli.Database("admin").RunCommand(context.Background(),
		bson.D{
			{Key: "currentOp", Value: 1},
			{Key: "filter", Value: bson.D{
				{Key: "transaction", Value: bson.D{{Key: "$exists", Value: true}}},
			}},
		}).Decode(&result)
	if err != nil {
		return -1
	}
	if inprog, ok := result["inprog"].(bson.A); ok {
		return int64(len(inprog))
	}
	return 0
}

// --- percentiles ---

func percentiles(data []int64) (p50, p95, p99, maxV int64) {
	if len(data) == 0 {
		return
	}
	sorted := make([]int64, len(data))
	copy(sorted, data)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := func(p float64) int {
		i := int(math.Ceil(p/100.0*float64(len(sorted)))) - 1
		if i < 0 {
			i = 0
		}
		return i
	}
	return sorted[idx(50)], sorted[idx(95)], sorted[idx(99)], sorted[len(sorted)-1]
}

func mean(data []int64) float64 {
	if len(data) == 0 {
		return 0
	}
	sum := int64(0)
	for _, v := range data {
		sum += v
	}
	return float64(sum) / float64(len(data))
}

// --- Result ---

type Result struct {
	Scenario        string  `json:"scenario"`
	GroupAWorkers   int     `json:"group_a_workers"`
	GroupBWorkers   int     `json:"group_b_workers"`
	GroupBTotalOps  int64   `json:"group_b_total_ops"`
	P50Ms           float64 `json:"p50_ms"`
	P95Ms           float64 `json:"p95_ms"`
	P99Ms           float64 `json:"p99_ms"`
	MaxMs           float64 `json:"max_ms"`
	MeanMs          float64 `json:"mean_ms"`
	PeakOpenTx      int64   `json:"peak_open_tx"`
	PeakInactiveTx  int64   `json:"peak_inactive_tx"`
	AbortedTxDelta        int64   `json:"aborted_tx_delta"`
	ReadTicketsOut        int64   `json:"peak_read_tickets_out"`
	WriteTicketsOut       int64   `json:"peak_write_tickets_out"`
	SnapshotOpenTx        int64   `json:"snapshot_open_tx"`        // explicit snapshot before Group B
	SnapshotInactiveTx    int64   `json:"snapshot_inactive_tx"`    // explicit snapshot before Group B
}

// --- peak monitor ---

type PeakMonitor struct {
	openTx     atomic.Int64
	inactiveTx atomic.Int64
	readTick   atomic.Int64
	writeTick  atomic.Int64
	done       chan struct{}
}

func startMonitor(cli *mongo.Client) *PeakMonitor {
	m := &PeakMonitor{done: make(chan struct{})}
	go func() {
		for {
			select {
			case <-m.done:
				return
			case <-time.After(10 * time.Millisecond):
				s := takeSnapshot(cli)
				if s.CurrentOpen > m.openTx.Load() {
					m.openTx.Store(s.CurrentOpen)
				}
				if s.CurrentInactive > m.inactiveTx.Load() {
					m.inactiveTx.Store(s.CurrentInactive)
				}
				if s.ReadTicketsOut > m.readTick.Load() {
					m.readTick.Store(s.ReadTicketsOut)
				}
				if s.WriteTicketsOut > m.writeTick.Load() {
					m.writeTick.Store(s.WriteTicketsOut)
				}
			}
		}
	}()
	return m
}

func (m *PeakMonitor) Stop() { close(m.done) }

// --- setup ---

func setupCollections(cli *mongo.Client) {
	ctx := context.Background()
	db := cli.Database(dbName)
	db.Collection(hotCollName).Drop(ctx)
	db.Collection(coldCollName).Drop(ctx)

	cold := db.Collection(coldCollName)
	docs := make([]interface{}, coldDocCount)
	for i := 0; i < coldDocCount; i++ {
		docs[i] = bson.D{{Key: "_id", Value: i}, {Key: "name", Value: fmt.Sprintf("product_%d", i)}}
	}
	cold.InsertMany(ctx, docs)
	fmt.Printf("  Setup: %d docs in %s\n", coldDocCount, coldCollName)
}

// --- Group B: simple reads ---

func runGroupB(cli *mongo.Client, workers, ops int) ([]int64, int64) {
	ctx := context.Background()
	coll := cli.Database(dbName).Collection(coldCollName)

	var mu sync.Mutex
	var allLatencies []int64
	var totalOps atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			local := make([]int64, 0, ops)
			for j := 0; j < ops; j++ {
				t0 := time.Now()
				res := coll.FindOne(ctx, bson.D{{Key: "_id", Value: (wid*ops + j) % coldDocCount}})
				if res.Err() == nil {
					totalOps.Add(1)
				}
				local = append(local, time.Since(t0).Milliseconds())
			}
			mu.Lock()
			allLatencies = append(allLatencies, local...)
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	return allLatencies, totalOps.Load()
}

// --- Scenario 1: Pure Baseline ---

func runPureBaseline(cli *mongo.Client, groupBWorkers int) Result {
	before := takeSnapshot(cli)
	mon := startMonitor(cli)

	latencies, ops := runGroupB(cli, groupBWorkers, opsPerWorker)

	mon.Stop()
	after := takeSnapshot(cli)
	p50, p95, p99, maxV := percentiles(latencies)

	return Result{
		Scenario:        "pure_baseline",
		GroupAWorkers:   0,
		GroupBWorkers:   groupBWorkers,
		GroupBTotalOps:  ops,
		P50Ms:           float64(p50),
		P95Ms:           float64(p95),
		P99Ms:           float64(p99),
		MaxMs:           float64(maxV),
		MeanMs:          mean(latencies),
		PeakOpenTx:      mon.openTx.Load(),
		PeakInactiveTx:  mon.inactiveTx.Load(),
		AbortedTxDelta:  after.TotalAborted - before.TotalAborted,
		ReadTicketsOut:  mon.readTick.Load(),
		WriteTicketsOut: mon.writeTick.Load(),
		SnapshotOpenTx:     0,
		SnapshotInactiveTx: 0,
	}
}

// --- Scenario 2/3: Mixed ---
// Group A workers each hold one open, inactive transaction.
// Coordination via per-worker channels:
//   ready[i]: Group A worker i signals it has entered the inactive phase
//   done[i]:  main signals Group A worker i to abort and exit

func runMixed(cli *mongo.Client, groupAWorkers, groupBWorkers int) Result {
	ctx := context.Background()

	type workerState struct {
		ready chan struct{}
		done  chan struct{}
	}
	states := make([]workerState, groupAWorkers)
	for i := range states {
		states[i] = workerState{
			ready: make(chan struct{}),
			done:  make(chan struct{}),
		}
	}

	var groupAWg sync.WaitGroup
	hotColl := cli.Database(dbName).Collection(hotCollName)

	for i := 0; i < groupAWorkers; i++ {
		groupAWg.Add(1)
		go func(wid int) {
			defer groupAWg.Done()

			sess, err := cli.StartSession()
			if err != nil {
				close(states[wid].ready) // unblock main on error
				return
			}
			defer sess.EndSession(ctx)

			// WithTransaction: commit on nil return, abort on non-transient error.
			_, _ = sess.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
				// First DB op — transaction becomes "active" briefly
				_, err := hotColl.InsertOne(ctx, bson.D{
					{Key: "wid", Value: wid},
					{Key: "ts", Value: time.Now().UnixMilli()},
				})
				if err != nil {
					return nil, err
				}

				// Signal: transaction is now open and inactive
				close(states[wid].ready)

				// Hold transaction open (inactive) until measurement is complete
				<-states[wid].done

				return nil, errAbortOnPurpose
			})
		}(i)
	}

	// Wait for all Group A workers to enter inactive phase
	for i := range states {
		<-states[i].ready
	}

	// Snapshot at measurement start
	currentOpCount := countCurrentOpTx(cli)
	s := takeSnapshot(cli)
	fmt.Printf("  [snapshot] open=%d inactive=%d currentOp_tx=%d\n",
		s.CurrentOpen, s.CurrentInactive, currentOpCount)

	// Measure Group B latency while Group A is inactive
	before := takeSnapshot(cli)
	mon := startMonitor(cli)

	latencies, ops := runGroupB(cli, groupBWorkers, opsPerWorker)

	mon.Stop()
	after := takeSnapshot(cli)

	// Release Group A workers to abort
	for i := range states {
		close(states[i].done)
	}
	groupAWg.Wait()

	p50, p95, p99, maxV := percentiles(latencies)

	return Result{
		Scenario:        fmt.Sprintf("mixed_groupA_%d", groupAWorkers),
		GroupAWorkers:   groupAWorkers,
		GroupBWorkers:   groupBWorkers,
		GroupBTotalOps:  ops,
		P50Ms:           float64(p50),
		P95Ms:           float64(p95),
		P99Ms:           float64(p99),
		MaxMs:           float64(maxV),
		MeanMs:          mean(latencies),
		PeakOpenTx:         mon.openTx.Load(),
		PeakInactiveTx:     mon.inactiveTx.Load(),
		AbortedTxDelta:     after.TotalAborted - before.TotalAborted,
		ReadTicketsOut:     mon.readTick.Load(),
		WriteTicketsOut:    mon.writeTick.Load(),
		SnapshotOpenTx:     s.CurrentOpen,
		SnapshotInactiveTx: s.CurrentInactive,
	}
}

// --- main ---

func saveJSON(results []Result, path string) {
	b, _ := json.MarshalIndent(results, "", "  ")
	_ = os.WriteFile(path, b, 0644)
}

func printResult(r Result) {
	fmt.Printf("  [%-30s] groupA=%3d groupB=%3d ops=%4d | p50=%5.1f p95=%5.1f p99=%6.1f max=%6.1f | snap_open=%d snap_inactive=%d r_tick=%d w_tick=%d\n",
		r.Scenario, r.GroupAWorkers, r.GroupBWorkers, r.GroupBTotalOps,
		r.P50Ms, r.P95Ms, r.P99Ms, r.MaxMs,
		r.SnapshotOpenTx, r.SnapshotInactiveTx,
		r.ReadTicketsOut, r.WriteTicketsOut,
	)
}

func main() {
	cli, err := mongo.Connect(options.Client().ApplyURI(mongoURI))
	if err != nil {
		panic(err)
	}
	defer cli.Disconnect(context.Background())

	if err := cli.Ping(context.Background(), nil); err != nil {
		fmt.Fprintf(os.Stderr, "MongoDB 연결 실패: %v\n레플리카셋 URI를 확인하세요: %s\n", err, mongoURI)
		os.Exit(1)
	}

	fmt.Println("=== MongoDB Transaction Collateral Damage Bench ===")
	fmt.Println("검증 명제: inactive transaction이 무관한 read 작업의 latency에 영향을 주는가?")
	fmt.Println()

	setupCollections(cli)
	fmt.Println()

	var results []Result

	// Baseline
	fmt.Println("--- Scenario 1: Pure Baseline (Group A = 0, Group B = 50) ---")
	r0 := runPureBaseline(cli, 50)
	printResult(r0)
	results = append(results, r0)
	time.Sleep(2 * time.Second)

	// Ladder: Group A 증가 (10 → 50 → 100 → 200)
	for _, groupACount := range []int{10, 50, 100, 200} {
		fmt.Printf("\n--- Scenario: Mixed (Group A = %d inactive tx, Group B = 50) ---\n", groupACount)
		r := runMixed(cli, groupACount, 50)
		printResult(r)
		results = append(results, r)
		time.Sleep(3 * time.Second)
	}

	// Summary table
	fmt.Println("\n=== Collateral Damage Summary ===")
	fmt.Printf("%-35s | %5s | %5s | %6s | %6s | %8s | %8s\n",
		"scenario", "p50", "p95", "p99", "max", "open_tx", "inactive")
	fmt.Println("----------------------------------+-------+-------+--------+--------+----------+----------")
	for _, r := range results {
		fmt.Printf("%-35s | %5.1f | %5.1f | %6.1f | %6.1f | %8d | %8d\n",
			r.Scenario, r.P50Ms, r.P95Ms, r.P99Ms, r.MaxMs,
			r.PeakOpenTx, r.PeakInactiveTx,
		)
	}

	_ = os.MkdirAll("../results", 0755)
	saveJSON(results, "../results/collateral.json")
	fmt.Println("\n결과 저장: results/collateral.json")
}
