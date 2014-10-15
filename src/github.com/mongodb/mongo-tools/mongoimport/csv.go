package mongoimport

import (
	"fmt"
	"github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/common/util"
	"github.com/mongodb/mongo-tools/mongoimport/csv"
	"gopkg.in/mgo.v2/bson"
	"io"
	"strings"
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
	// internal synchronization helpers
	isProcessing    bool
	startProcessing chan bool
	hasProcessed    chan bool
	// document is used to hold the processed CSV document as a bson.D
	csvOut bson.D
	// document is used to hold the input CSV document to process
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
// readChan channel and if any error is encountered, that is sent in the errChan
// channel. It keeps reading from the underlying input source until it hits EOF
// or an error
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
		csvInputReader.processInSequence(csvRecordChan, readChan)
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
				csvInputReader.sendCSV(csvRecordChan, readChan)
			}()
		}
		wg.Wait()
	}
	close(readChan)
}

// sendCSV reads from the csvRecordChan and for each read record, converts it to
// a bson.D document before sending it on the readChan channel
func (csvInputReader *CSVInputReader) sendCSV(csvRecordChan chan []string, readChan chan bson.D) {
	for csvRecord := range csvRecordChan {
		readChan <- csvInputReader.csvRecordToBSON(csvRecord)
	}
}

// csvRecordToBSON reads in slice of CSV record and returns a BSON document based
// on the record.
func (csvInputReader *CSVInputReader) csvRecordToBSON(csvRecord []string) (document bson.D) {
	log.Logf(2, "got line: %v", csvRecord)
	var parsedValue interface{}
	for index, token := range csvRecord {
		parsedValue = getParsedValue(token)
		if index < len(csvInputReader.Fields) {
			// for nested fields - in the form "a.b.c", ensure
			// that the value is set accordingly
			if strings.Index(csvInputReader.Fields[index], ".") != -1 {
				setNestedValue(csvInputReader.Fields[index], parsedValue, &document)
			} else {
				document = append(document, bson.DocElem{csvInputReader.Fields[index], parsedValue})
			}
		} else {
			key := "field" + string(index)
			if util.StringSliceContains(csvInputReader.Fields, key) {
				panic(fmt.Sprintf("Duplicate header name - on %v - for token #%v ('%v') in document #%v",
					key, index+1, parsedValue, csvInputReader.numProcessed))
			}
			document = append(document, bson.DocElem{csvInputReader.Fields[index], parsedValue})
		}
	}
	return
}

// processInSequence concurrently processes data gotten from the csvRecordChan
// channel in parallel and then sends over the processed data to the readChan
// channel in sequence in which the data was received
func (csvInputReader *CSVInputReader) processInSequence(csvRecordChan chan []string, readChan chan bson.D) {
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
			go csvWorkerThread[i].processCSV()
		}
		csvWorkerThread[i].csvIn = csvRecord
		csvWorkerThread[i].startProcessing <- true
		i = (i + 1) % numProcessingThreads
	}
	// drain any thread that's still in the middle of processing
	for i := 0; i < numProcessingThreads; i++ {
		if csvWorkerThread[i].isProcessing {
			<-csvWorkerThread[i].hasProcessed
			readChan <- csvWorkerThread[i].csvOut
			close(csvWorkerThread[i].startProcessing)
		}
	}
}

// processCSV waits on the startProcessing channel to process data and sends
// a signal when it's done processing
func (csvInputReader *CSVInputReader) processCSV() {
	for <-csvInputReader.startProcessing {
		csvInputReader.csvOut = csvRecordToBSON(csvInputReader.csvIn)
		csvInputReader.hasProcessed <- true
	}
}
