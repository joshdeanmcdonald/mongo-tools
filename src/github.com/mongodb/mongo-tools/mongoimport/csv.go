package mongoimport

import (
	"fmt"
	"github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/mongoimport/csv"
	"gopkg.in/mgo.v2/bson"
	"io"
	"sync"
)

// CSVInputReader is a struct that implements the InputReader interface for a
// CSV input source
type CSVInputReader struct {
	// Fields is a list of field names in the BSON documents to be imported
	Fields []string
	// csvReader is the underlying reader used to read data in from the CSV
	// or TSV file
	csvReader *csv.Reader
	// numProcessed indicates the number of CSV documents processed
	numProcessed int64
	// csvRecord stores each line of input we read from the underlying reader
	csvRecord []string
	/* internal synchronization helpers for sequential inserts */
	// indicates a goroutine is currently processing a record
	isProcessing bool
	// indicates a waiting goroutine can commence processing a record
	startProcessing chan bool
	// indicates a goroutine has completed processing a record
	hasProcessed chan bool
	// document is used to hold each worker's processed CSV document as a bson.D
	csvOut bson.D
	// slice is used to hold the input CSV document for a worker to process
	csvIn []string
}

// NewCSVInputReader returns a CSVInputReader configured to read input from the
// given io.Reader, extracting the specified fields only.
func NewCSVInputReader(fields []string, in io.Reader) *CSVInputReader {
	csvReader := csv.NewReader(in)
	// allow variable number of fields in document
	csvReader.FieldsPerRecord = -1
	csvReader.TrimLeadingSpace = true
	return &CSVInputReader{
		Fields:    fields,
		csvReader: csvReader,
	}
}

// SetHeader sets the header field for a CSV
func (csvInputReader *CSVInputReader) SetHeader(hasHeaderLine bool) (err error) {
	fields, err := validateHeaders(csvInputReader, hasHeaderLine)
	if err != nil {
		return err
	}
	csvInputReader.Fields = fields
	return nil
}

// GetHeaders returns the current header fields for a CSV importer
func (csvInputReader *CSVInputReader) GetHeaders() []string {
	return csvInputReader.Fields
}

// ReadHeadersFromSource reads the header field from the CSV importer's reader
func (csvInputReader *CSVInputReader) ReadHeadersFromSource() ([]string, error) {
	return csvInputReader.csvReader.Read()
}

// StreamDocument takes in two channels: it sends processed documents on the
// readChan channel and if any error is encountered, the error is sent on the
// errChan channel. It keeps reading from the underlying input source until it hits EOF
// hits EOF or an error
func (csvInputReader *CSVInputReader) StreamDocument(readChan chan bson.D, errChan chan error) {
	csvRecordChan := make(chan []string, numProcessingThreads)
	var err error

	go func() {
		for {
			csvInputReader.csvRecord, err = csvInputReader.csvReader.Read()
			if err != nil {
				close(csvRecordChan)
				if err == io.EOF {
					errChan <- err
					return
				}
				csvInputReader.numProcessed++
				errChan <- fmt.Errorf("read error on entry #%v: %v", csvInputReader.numProcessed, err)
				return
			}
			csvRecordChan <- csvInputReader.csvRecord
			csvInputReader.numProcessed++
		}
	}()

	if maintainInsertionOrder {
		csvInputReader.sequentialTSVStream(csvRecordChan, readChan)
	} else {
		wg := &sync.WaitGroup{}
		for i := 0; i < numProcessingThreads; i++ {
			wg.Add(1)
			go func() {
				defer func() {
					if r := recover(); r != nil {
						log.Logf(0, "error decoding CSV: %v", r)
					}
					wg.Done()
				}()
				csvInputReader.concurrentCSVStream(csvRecordChan, readChan)
			}()
		}
		wg.Wait()
	}
	close(readChan)
}

// sequentialTSVStream concurrently processes data gotten from the csvRecordChan
// channel in parallel and then sends over the processed data to the readChan
// channel in sequence in which the data was received
func (csvInputReader *CSVInputReader) sequentialTSVStream(csvRecordChan chan []string, readChan chan bson.D) {
	var csvWorkerThread []*CSVInputReader
	// initialize all our concurrent processing threads
	for i := 0; i < numProcessingThreads; i++ {
		csvWorker := &CSVInputReader{
			Fields:          csvInputReader.Fields,
			hasProcessed:    make(chan bool),
			startProcessing: make(chan bool),
		}
		csvWorkerThread = append(csvWorkerThread, csvWorker)
	}
	i := 0
	// feed in the CSV line to be processed and do round-robin
	// reads from each worker once processing is completed
	for csvRecord := range csvRecordChan {
		if csvWorkerThread[i].isProcessing {
			<-csvWorkerThread[i].hasProcessed
			readChan <- csvWorkerThread[i].csvOut
		} else {
			csvWorkerThread[i].isProcessing = true
			go csvWorkerThread[i].doConcurrentProcess()
		}
		csvWorkerThread[i].csvIn = csvRecord
		csvWorkerThread[i].startProcessing <- true
		i = (i + 1) % numProcessingThreads
	}
	// drain any threads that're still in the middle of processing
	for i := 0; i < numProcessingThreads; i++ {
		if csvWorkerThread[i].isProcessing {
			<-csvWorkerThread[i].hasProcessed
			readChan <- csvWorkerThread[i].csvOut
			close(csvWorkerThread[i].startProcessing)
		}
	}
}

// concurrentCSVStream reads from the csvRecordChan and for each read record,
// converts it to a bson.D document before sending it on the readChan channel
func (csvInputReader *CSVInputReader) concurrentCSVStream(csvRecordChan chan []string, readChan chan bson.D) {
	for csvRecord := range csvRecordChan {
		readChan <- csvInputReader.csvRecordToBSON(csvRecord)
	}
}

// csvRecordToBSON reads a CSV record and returns a BSON document constructed
// from the CSV input source's fields and the given record
func (csvInputReader *CSVInputReader) csvRecordToBSON(csvRecord []string) (document bson.D) {
	return tokensToBSON(csvInputReader.Fields, csvInputReader.csvIn, csvInputReader.numProcessed)
}

// doConcurrentProcess waits on the startProcessing channel to process data and
// sends a signal when it's done processing
func (csvInputReader *CSVInputReader) doConcurrentProcess() {
	for <-csvInputReader.startProcessing {
		csvInputReader.csvOut = tokensToBSON(
			csvInputReader.Fields,
			csvInputReader.csvIn,
			csvInputReader.numProcessed,
		)
		csvInputReader.hasProcessed <- true
	}
}
