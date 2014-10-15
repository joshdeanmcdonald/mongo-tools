package mongoimport

import (
	"errors"
	"fmt"
	"github.com/mongodb/mongo-tools/common/bsonutil"
	"github.com/mongodb/mongo-tools/common/json"
	"github.com/mongodb/mongo-tools/common/log"
	"gopkg.in/mgo.v2/bson"
	"io"
	"strings"
	"sync"
)

// JSONInputReader is an implementation of InputReader that reads documents
// in JSON format.
type JSONInputReader struct {
	// IsArray indicates if the JSON import is an array of JSON documents
	// or not
	IsArray bool
	// Decoder is used to read the next valid JSON documents from the input source
	Decoder *json.Decoder
	// numProcessed indicates the number of JSON documents processed
	numProcessed int64
	// readOpeningBracket indicates if the underlying io.Reader has consumed
	// an opening bracket from the input source. Used to prevent errors when
	// a JSON input source contains just '[]'
	readOpeningBracket bool
	// expectedByte is used to store the next expected valid character for JSON
	// array imports
	expectedByte byte
	// bytesFromReader is used to store the next byte read from the Reader for
	// JSON array imports
	bytesFromReader []byte
	// separatorReader is used for JSON arrays to look for a valid array
	// separator. It is a reader consisting of the decoder's buffer and the
	// underlying reader
	separatorReader io.Reader
	/* internal synchronization helpers for sequential inserts */
	// indicates a goroutine is currently processing a record
	isProcessing bool
	// indicates a waiting goroutine can commence processing a record
	startProcessing chan bool
	// indicates a goroutine has completed processing a record
	hasProcessed chan bool
	// document is used to hold each worker's processed TSV document as a bson.D
	jsonOut bson.D
	// string is used to hold the input TSV record for a worker to process
	jsonIn []byte
}

const (
	JSON_ARRAY_START = '['
	JSON_ARRAY_SEP   = ','
	JSON_ARRAY_END   = ']'
)

var (
	// ErrNoOpeningBracket means that the input source did not contain any
	// opening brace - returned only if --jsonArray is passed in.
	ErrNoOpeningBracket = errors.New("bad JSON array format - found no " +
		"opening bracket '[' in input source")

	// ErrNoClosingBracket means that the input source did not contain any
	// closing brace - returned only if --jsonArray is passed in.
	ErrNoClosingBracket = errors.New("bad JSON array format - found no " +
		"closing bracket ']' in input source")
)

// NewJSONInputReader creates a new JSONInputReader in array mode if specified,
// configured to read data to the given io.Reader
func NewJSONInputReader(isArray bool, in io.Reader) *JSONInputReader {
	return &JSONInputReader{
		IsArray:            isArray,
		Decoder:            json.NewDecoder(in),
		readOpeningBracket: false,
		bytesFromReader:    make([]byte, 1),
	}
}

// SetHeader is a no-op for JSON imports
func (jsonInputReader *JSONInputReader) SetHeader(hasHeaderLine bool) error {
	return nil
}

// GetHeaders is a no-op for JSON imports
func (jsonInputReader *JSONInputReader) GetHeaders() []string {
	return nil
}

// ReadHeadersFromSource is a no-op for JSON imports
func (jsonInputReader *JSONInputReader) ReadHeadersFromSource() ([]string, error) {
	return nil, nil
}

// StreamDocument takes in two channels: it sends processed documents on the
// readChan channel and if any error is encountered, the error is sent on the
// errChan channel. It keeps reading from the underlying input source until it hits EOF
// hits EOF or an error
func (jsonInputReader *JSONInputReader) StreamDocument(readChan chan bson.D, errChan chan error) {
	rawChan := make(chan []byte, numProcessingThreads)
	var err error
	go func() {
		for {
			if jsonInputReader.IsArray {
				if err = jsonInputReader.readJSONArraySeparator(); err != nil {
					close(rawChan)
					if err == io.EOF {
						errChan <- err
						return
					}
					jsonInputReader.numProcessed++
					errChan <- fmt.Errorf("error reading separator after document #%v: %v", jsonInputReader.numProcessed, err)
					return
				}
			}
			rawBytes, err := jsonInputReader.Decoder.ScanObject()
			if err != nil {
				close(rawChan)
				errChan <- err
				return
			}
			rawChan <- rawBytes
			jsonInputReader.numProcessed++
		}
	}()

	if maintainInsertionOrder {
		jsonInputReader.sequentialJSONStream(rawChan, readChan)
	} else {
		wg := &sync.WaitGroup{}
		for i := 0; i < numProcessingThreads; i++ {
			wg.Add(1)
			go func() {
				defer func() {
					if r := recover(); r != nil {
						log.Logf(0, "error decoding JSON: %v", r)
					}
					wg.Done()
				}()
				jsonInputReader.concurrentJSONStream(rawChan, readChan)
			}()
		}
		wg.Wait()
	}
	close(readChan)
}

// sequentialJSONStream concurrently processes data gotten from the rawBytesChan
// channel in parallel and then sends over the processed data to the readChan
// channel in sequence in which the data was received
func (jsonInputReader *JSONInputReader) sequentialJSONStream(rawBytesChan chan []byte, readChan chan bson.D) {
	var jsonWorkerThread []*JSONInputReader
	// initialize all our concurrent processing threads
	for i := 0; i < numProcessingThreads; i++ {
		jsonWorker := &JSONInputReader{
			hasProcessed:    make(chan bool),
			startProcessing: make(chan bool),
		}
		jsonWorkerThread = append(jsonWorkerThread, jsonWorker)
	}
	i := 0
	// feed in the JSON document to be processed and do round-robin
	// reads from each worker once processing is completed
	for jsonRecord := range rawBytesChan {
		if jsonWorkerThread[i].isProcessing {
			<-jsonWorkerThread[i].hasProcessed
			readChan <- jsonWorkerThread[i].jsonOut
		} else {
			jsonWorkerThread[i].isProcessing = true
			go jsonWorkerThread[i].doConcurrentProcess()
		}
		jsonWorkerThread[i].jsonIn = jsonRecord
		jsonWorkerThread[i].startProcessing <- true
		i = (i + 1) % numProcessingThreads
	}
	// drain any threads that're still in the middle of processing
	for i := 0; i < numProcessingThreads; i++ {
		if jsonWorkerThread[i].isProcessing {
			<-jsonWorkerThread[i].hasProcessed
			readChan <- jsonWorkerThread[i].jsonOut
			close(jsonWorkerThread[i].startProcessing)
		}
	}
}

// concurrentJSONStream reads from the jsonRecordChan and for each read record,
// converts it to a bson.D document before sending it on the readChan channel
func (jsonInputReader *JSONInputReader) concurrentJSONStream(jsonRecordChan chan []byte, readChan chan bson.D) {
	for jsonRecord := range jsonRecordChan {
		readChan <- jsonInputReader.jsonRecordToBSON(jsonRecord)
	}
}

// doConcurrentProcess waits on the startProcessing channel to process data and
// sends a signal when it's done processing
func (jsonInputReader *JSONInputReader) doConcurrentProcess() {
	for <-jsonInputReader.startProcessing {
		jsonInputReader.jsonOut = jsonInputReader.jsonRecordToBSON(jsonInputReader.jsonIn)
		jsonInputReader.hasProcessed <- true
	}
}

// jsonRecordToBSON reads in a byte slice and creates a BSON document - based on
// the record - which is then returned
func (jsonInputReader *JSONInputReader) jsonRecordToBSON(rawBytes []byte) (bsonD bson.D) {
	document, err := json.UnmarshalBsonD(rawBytes)
	if err != nil {
		panic(fmt.Sprintf("error unmarshalling bytes on document #%v: %v", jsonInputReader.numProcessed, err))
	}
	log.Logf(2, "got line: %v", document)
	// TODO: perhaps move this to decode.go
	if bsonD, err = bsonutil.GetExtendedBsonD(document); err != nil {
		panic(fmt.Sprintf("error getting extended BSON for document #%v: %v", jsonInputReader.numProcessed, err))
	}
	log.Logf(3, "got extended line: %#v", bsonD)
	return
}

// readJSONArraySeparator is a helper method used to process JSON arrays. It is
// used to read any of the valid separators for a JSON array and flag invalid
// characters.
//
// It will read a byte at a time until it finds an expected character after
// which it returns control to the caller.
//
// It will also return immediately if it finds any error (including EOF). If it
// reads a JSON_ARRAY_END byte, as a validity check it will continue to scan the
// input source until it hits an error (including EOF) to ensure the entire
// input source content is a valid JSON array
func (jsonInputReader *JSONInputReader) readJSONArraySeparator() error {
	jsonInputReader.expectedByte = JSON_ARRAY_SEP
	if jsonInputReader.numProcessed == 0 {
		jsonInputReader.expectedByte = JSON_ARRAY_START
	}

	var readByte byte
	scanp := 0
	jsonInputReader.separatorReader = io.MultiReader(
		jsonInputReader.Decoder.Buffered(),
		jsonInputReader.Decoder.R,
	)
	for readByte != jsonInputReader.expectedByte {
		n, err := jsonInputReader.separatorReader.Read(jsonInputReader.bytesFromReader)
		scanp += n
		if n == 0 || err != nil {
			if err == io.EOF {
				return ErrNoClosingBracket
			}
			return err
		}
		readByte = jsonInputReader.bytesFromReader[0]

		if readByte == JSON_ARRAY_END {
			// if we read the end of the JSON array, ensure we have no other
			// non-whitespace characters at the end of the array
			for {
				_, err = jsonInputReader.separatorReader.Read(jsonInputReader.bytesFromReader)
				if err != nil {
					// takes care of the '[]' case
					if !jsonInputReader.readOpeningBracket {
						return ErrNoOpeningBracket
					}
					return err
				}
				readString := string(jsonInputReader.bytesFromReader[0])
				if strings.TrimSpace(readString) != "" {
					return fmt.Errorf("bad JSON array format - found '%v' "+
						"after '%v' in input source", readString,
						string(JSON_ARRAY_END))
				}
			}
		}

		// this will catch any invalid inter JSON object byte that occurs in the
		// input source
		if !(readByte == JSON_ARRAY_SEP ||
			strings.TrimSpace(string(readByte)) == "" ||
			readByte == JSON_ARRAY_START ||
			readByte == JSON_ARRAY_END) {
			if jsonInputReader.expectedByte == JSON_ARRAY_START {
				return ErrNoOpeningBracket
			}
			return fmt.Errorf("bad JSON array format - found '%v' outside "+
				"JSON object/array in input source", string(readByte))
		}
	}
	// adjust the buffer to account for read bytes
	if scanp < len(jsonInputReader.Decoder.Buf) {
		jsonInputReader.Decoder.Buf = jsonInputReader.Decoder.Buf[scanp:]
	} else {
		jsonInputReader.Decoder.Buf = []byte{}
	}
	jsonInputReader.readOpeningBracket = true
	return nil
}
