package mongorestore

import (
	"container/heap"
	"sort"
)

type PriorityType int

const (
	Legacy PriorityType = iota + 1
	MultiDatabaseLTF
)

// IntentPrioritizer encapsulates the logic of scheduling intents
// for restoration. It can know about which intents are in the
// process of being restored through the "Finish" hook.
//
// Oplog entries and auth entries are not handled by the prioritizer,
// as these are special cases handled by the regular mongorestore code.
// TODO flesh this docstring out
type IntentPrioritizer interface {
	Get() *Intent
	Finish(*Intent)
}

//===== Legacy =====

type legacyPrioritizer struct {
	queue []*Intent
}

// Make sure this implements the interface
var _ IntentPrioritizer = &legacyPrioritizer{}

func NewLegacyPrioritizer(intentList []*Intent) *legacyPrioritizer {
	return &legacyPrioritizer{queue: intentList}
}

func (legacy *legacyPrioritizer) Get() *Intent {
	var intent *Intent

	if len(legacy.queue) == 0 {
		return nil
	}

	intent, legacy.queue = legacy.queue[0], legacy.queue[1:]
	return intent
}

func (legacy *legacyPrioritizer) Finish(*Intent) {
	// no-op
	return
}

//===== Multi Database Longest Task First =====

// multiDatabaseLTF is designed to properly schedule intents with two constraints:
//  1. it is optimized to run in a multi-processor environment
//  2. it is optimized for parallelism against 2.6's db-level write lock
// These goals result in a design that attempts to have as many different
// database's intents being restored as possible and attempts to restore the
// largest collections first.
//
// If we can have a minimum number of collections in flight for a given db,
// we avoid lock contention in an optimal way on 2.6 systems. That is,
// it is better to have two restore jobs where
//  job1 = "test.mycollection"
//  job2 = "mydb2.othercollection"
// so that these collections do not compete for the db-level write lock.
//
// We also schedule the largest jobs first, in a greedy fashion, in order
// to minimize total restoration time. Each database's intents are sorted
// by decreasing file size at initialization, so that the largest jobs are
// run first. Admittedly, .bson file size is not a direct predictor of restore
// time, but there is certainly a strong correlation. Note that this attribute
// is secondary to the multi-db scheduling laid out above, since multi-db will
// get us bigger wins in terms of parallelism.
type multiDatabaseLTFPrioritizer struct {
	dbHeap     heap.Interface
	counterMap map[string]*dbCounter
}

// Make sure this implements the interface
var _ IntentPrioritizer = &multiDatabaseLTFPrioritizer{}

// NewMultiDatabaseLTFPrioritizer takes in a list of intents and returns an
// initialized prioritizer.
func NewMultiDatabaseLTFPrioritizer(intents []*Intent) *multiDatabaseLTFPrioritizer {
	prioritizer := &multiDatabaseLTFPrioritizer{
		counterMap: map[string]*dbCounter{},
		dbHeap:     &DBHeap{},
	}
	heap.Init(prioritizer.dbHeap)
	// first, create all database counters
	for _, intent := range intents {
		counter, exists := prioritizer.counterMap[intent.DB]
		if !exists {
			// initialize a new counter if one doesn't exist for DB
			counter = &dbCounter{}
			prioritizer.counterMap[intent.DB] = counter
		}
		counter.collections = append(counter.collections, intent)
	}
	// then ensure that all the dbCounters have sorted intents
	for _, counter := range prioritizer.counterMap {
		counter.SortCollectionsBySize()
		heap.Push(prioritizer.dbHeap, counter)
	}
	return prioritizer
}

// Get returns the next prioritized intent and updates the count of active
// restores for the returned intent's DB. Get is not thread safe, and depends
// on the implementation of the intent manager to lock around it.
func (mdb *multiDatabaseLTFPrioritizer) Get() *Intent {
	if mdb.dbHeap.Len() == 0 {
		// we're out of things to return
		return nil
	}
	optimalDB := heap.Pop(mdb.dbHeap).(*dbCounter)
	optimalDB.active++
	nextIntent := optimalDB.PopIntent()
	// only release the db counter if it's out of collections
	if len(optimalDB.collections) > 0 {
		heap.Push(mdb.dbHeap, optimalDB)
	}
	return nextIntent
}

// Finish decreases the number of active restore jobs for the given intent's
// database, and reshuffles the heap accordingly. Finish is  not thread safe,
// and depends on the implementation of the intent manager to lock around it.
func (mdb *multiDatabaseLTFPrioritizer) Finish(intent *Intent) {
	counter := mdb.counterMap[intent.DB]
	counter.active--
	// only fix up the heap if the counter is still in the heap
	if len(counter.collections) > 0 {
		// This is an O(n) operation on the heap. We could make all heap
		// operations O(log(n)) if we set up dbCounters to track their own
		// position in the heap, but in practice this overhead is likely negligible.
		heap.Init(mdb.dbHeap)
	}
}

type dbCounter struct {
	active      int
	collections []*Intent
}

func (dbc *dbCounter) SortCollectionsBySize() {
	sort.Sort(ByFilesize(dbc.collections))
}

func (dbc *dbCounter) PopIntent() *Intent {
	var intent *Intent
	if len(dbc.collections) > 0 {
		intent, dbc.collections = dbc.collections[0], dbc.collections[1:]
	}
	return intent
}

// For sorting intents from largest to smallest BSON filesize
type ByFilesize []*Intent

func (s ByFilesize) Len() int           { return len(s) }
func (s ByFilesize) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s ByFilesize) Less(i, j int) bool { return s[i].BSONSize > s[j].BSONSize }

// TODO LONG DESCRIPTION
// returns the largest collection of the databases with the least active restores
type DBHeap []*dbCounter

var _ heap.Interface = &DBHeap{} // ensure we implement heap

func (dbh DBHeap) Len() int      { return len(dbh) }
func (dbh DBHeap) Swap(i, j int) { dbh[i], dbh[j] = dbh[j], dbh[i] }
func (dbh DBHeap) Less(i, j int) bool {
	if dbh[i].active == dbh[j].active {
		// prioritize the largest bson file if dbs have the same number
		// of restorations in progress
		return dbh[i].collections[0].BSONSize > dbh[j].collections[0].BSONSize
	}
	return dbh[i].active < dbh[j].active
}

func (dbh *DBHeap) Push(x interface{}) {
	*dbh = append(*dbh, x.(*dbCounter))
}

func (dbh *DBHeap) Pop() interface{} {
	old := *dbh
	n := len(old)
	toPop := old[n-1]
	*dbh = old[0 : n-1]
	return toPop
}
