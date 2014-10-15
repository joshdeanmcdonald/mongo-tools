package mongoimport

import (
	"bufio"
	"fmt"
	"github.com/mongodb/mongo-tools/common/log"
	"gopkg.in/mgo.v2/bson"
	"io"
	"strings"
	"sync"
)

const (
	entryDelimiter = '\n'
	tokenSeparator = "\t"
)

// TSVInputReader is a struct that implements the InputReader interface for a
// TSV input source
type TSVInputReader struct {
	// Fields is a list of field names in the BSON documents to be imported
	Fields []string
	// tsvReader is the underlying reader used to read data in from the TSV
	// or TSV file
	tsvReader *bufio.Reader
	// numProcessed indicates the number of TSV documents processed
	numProcessed int64
	// tsvRecord stores each line of input we read from the underlying reader
	tsvRecord string
	/* internal synchronization helpers for sequential inserts */
	// indicates a goroutine is currently processing a record
	isProcessing bool
	// indicates a waiting goroutine can commence processing a record
	startProcessing chan bool
	// indicates a goroutine has completed processing a record
	hasProcessed chan bool
	// document is used to hold each worker's processed TSV document as a bson.D
	tsvOut bson.D
	// string is used to hold the input TSV record for a worker to process
	tsvIn string
}

// NewTSVInputReader returns a TSVInputReader configured to read input from the
// given io.Reader, extracting the specified fields only.
func NewTSVInputReader(fields []string, in io.Reader) *TSVInputReader {
	return &TSVInputReader{
		Fields:    fields,
		tsvReader: bufio.NewReader(in),
	}
}

// SetHeader sets the header field for a TSV
func (tsvInputReader *TSVInputReader) SetHeader(hasHeaderLine bool) (err error) {
	fields, err := validateHeaders(tsvInputReader, hasHeaderLine)
	if err != nil {
		return err
	}
	tsvInputReader.Fields = fields
	return nil
}

// GetHeaders returns the current header fields for a TSV importer
func (tsvInputReader *TSVInputReader) GetHeaders() []string {
	return tsvInputReader.Fields
}

// ReadHeadersFromSource reads the header field from the TSV importer's reader
func (tsvInputReader *TSVInputReader) ReadHeadersFromSource() ([]string, error) {
	unsortedHeaders := []string{}
	stringHeaders, err := tsvInputReader.tsvReader.ReadString(entryDelimiter)
	if err != nil {
		return nil, err
	}
	tokenizedHeaders := strings.Split(stringHeaders, tokenSeparator)
	for _, header := range tokenizedHeaders {
		unsortedHeaders = append(unsortedHeaders, strings.TrimSpace(header))
	}
	return unsortedHeaders, nil
}

// StreamDocument takes in two channels: it sends processed documents on the
// readChan channel and if any error is encountered, the error is sent on the
// errChan channel. It keeps reading from the underlying input source until it hits EOF
// hits EOF or an error
func (tsvInputReader *TSVInputReader) StreamDocument(readChan chan bson.D, errChan chan error) {
	tsvRecordChan := make(chan string, numProcessingThreads)
	var err error

	go func() {
		for {
			tsvInputReader.tsvRecord, err = tsvInputReader.tsvReader.ReadString(entryDelimiter)
			if err != nil {
				close(tsvRecordChan)
				if err == io.EOF {
					errChan <- err
					return
				}
				tsvInputReader.numProcessed++
				errChan <- fmt.Errorf("read error on entry #%v: %v", tsvInputReader.numProcessed, err)
				return
			}
			tsvRecordChan <- tsvInputReader.tsvRecord
			tsvInputReader.numProcessed++
		}
	}()

	if maintainInsertionOrder {
		tsvInputReader.sequentialTSVStream(tsvRecordChan, readChan)
	} else {
		wg := &sync.WaitGroup{}
		for i := 0; i < numProcessingThreads; i++ {
			wg.Add(1)
			go func() {
				defer func() {
					if r := recover(); r != nil {
						log.Logf(0, "error decoding TSV: %v", r)
					}
					wg.Done()
				}()
				tsvInputReader.concurrentTSVStream(tsvRecordChan, readChan)
			}()
		}
		wg.Wait()
	}
	close(readChan)
}

// sequentialTSVStream concurrently processes data gotten from the tsvRecordChan
// channel in parallel and then sends over the processed data to the readChan
// channel in sequence in which the data was received
func (tsvInputReader *TSVInputReader) sequentialTSVStream(tsvRecordChan chan string, readChan chan bson.D) {
	var tsvWorkerThread []*TSVInputReader
	// initialize all our concurrent processing threads
	for i := 0; i < numProcessingThreads; i++ {
		tsvWorker := &TSVInputReader{
			Fields:          tsvInputReader.Fields,
			hasProcessed:    make(chan bool),
			startProcessing: make(chan bool),
		}
		tsvWorkerThread = append(tsvWorkerThread, tsvWorker)
	}
	i := 0
	// feed in the TSV line to be processed and do round-robin
	// reads from each worker once processing is completed
	for tsvRecord := range tsvRecordChan {
		if tsvWorkerThread[i].isProcessing {
			<-tsvWorkerThread[i].hasProcessed
			readChan <- tsvWorkerThread[i].tsvOut
		} else {
			tsvWorkerThread[i].isProcessing = true
			go tsvWorkerThread[i].doConcurrentProcess()
		}
		tsvWorkerThread[i].tsvIn = tsvRecord
		tsvWorkerThread[i].startProcessing <- true
		i = (i + 1) % numProcessingThreads
	}
	// drain any threads that're still in the middle of processing
	for i := 0; i < numProcessingThreads; i++ {
		if tsvWorkerThread[i].isProcessing {
			<-tsvWorkerThread[i].hasProcessed
			readChan <- tsvWorkerThread[i].tsvOut
			close(tsvWorkerThread[i].startProcessing)
		}
	}
}

// concurrentTSVStream reads from the tsvRecordChan and for each read record, converts it to
// a bson.D document before sending it on the readChan channel
func (tsvInputReader *TSVInputReader) concurrentTSVStream(tsvRecordChan chan string, readChan chan bson.D) {
	for tsvRecord := range tsvRecordChan {
		readChan <- tsvInputReader.tsvRecordToBSON(tsvRecord)
	}
}

// tsvRecordToBSON reads a TSV record and returns a BSON document constructed
// from the TSV input source's fields and the given record
func (tsvInputReader *TSVInputReader) tsvRecordToBSON(tsvRecord string) bson.D {
	tsvTokens := strings.Split(strings.TrimRight(tsvRecord, "\r\n"), tokenSeparator)
	return tokensToBSON(tsvInputReader.Fields, tsvTokens, tsvInputReader.numProcessed)
}

// doConcurrentProcess waits on the startProcessing channel to process data and
// sends a signal when it's done processing
func (tsvInputReader *TSVInputReader) doConcurrentProcess() {
	for <-tsvInputReader.startProcessing {
		tsvInputReader.tsvOut = tsvInputReader.tsvRecordToBSON(tsvInputReader.tsvIn)
		tsvInputReader.hasProcessed <- true
	}
}
