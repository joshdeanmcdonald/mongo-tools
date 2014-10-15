package mongoimport

import (
	"fmt"
	"github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/common/util"
	"gopkg.in/mgo.v2/bson"
	"sort"
	"strconv"
	"strings"
)

// tokensToBSON reads in slice of records - along with ordered fields names -
// and returns a BSON document for the record.
func tokensToBSON(fields, tokens []string, numProcessed int64) (document bson.D) {
	log.Logf(2, "got line: %v", tokens)
	var parsedValue interface{}
	for index, token := range tokens {
		parsedValue = getParsedValue(token)
		if index < len(fields) {
			if strings.Index(fields[index], ".") != -1 {
				setNestedValue(fields[index], parsedValue, &document)
			} else {
				document = append(document, bson.DocElem{fields[index], parsedValue})
			}
		} else {
			key := "field" + string(index)
			if util.StringSliceContains(fields, key) {
				panic(fmt.Sprintf("Duplicate header name - on %v - for token #%v ('%v') in document #%v",
					key, index+1, parsedValue, numProcessed))
			}
			document = append(document, bson.DocElem{key, parsedValue})
		}
	}
	return
}

// constructUpsertDocument constructs a BSON document to use for upserts
func constructUpsertDocument(upsertFields []string, document bson.M) bson.M {
	upsertDocument := bson.M{}
	var hasDocumentKey bool
	for _, key := range upsertFields {
		upsertDocument[key] = getUpsertValue(key, document)
		if upsertDocument[key] != nil {
			hasDocumentKey = true
		}
	}
	if !hasDocumentKey {
		return nil
	}
	return upsertDocument
}

// getParsedValue returns the appropriate concrete type for the given token
// it first attempts to convert it to an int, if that doesn't succeed, it
// attempts conversion to a float, if that doesn't succeed, it returns the
// token as is.
func getParsedValue(token string) interface{} {
	parsedInt, err := strconv.Atoi(token)
	if err == nil {
		return parsedInt
	}
	parsedFloat, err := strconv.ParseFloat(token, 64)
	if err == nil {
		return parsedFloat
	}
	return token
}

// getUpsertValue takes a given BSON document and a given field, and returns the
// field's associated value in the document. The field is specified using dot
// notation for nested fields. e.g. "person.age" would return 34 would return
// 34 in the document: bson.M{"person": bson.M{"age": 34}} whereas,
// "person.name" would return nil
func getUpsertValue(field string, document bson.M) interface{} {
	index := strings.Index(field, ".")
	if index == -1 {
		return document[field]
	}
	left := field[0:index]
	if document[left] == nil {
		return nil
	}
	subDoc, ok := document[left].(bson.M)
	if !ok {
		return nil
	}
	return getUpsertValue(field[index+1:], subDoc)
}

// removeBlankFields removes empty/blank fields in csv and tsv
func removeBlankFields(document bson.D) bson.D {
	for index, pair := range document {
		if _, ok := pair.Value.(string); ok && pair.Value.(string) == "" {
			document = append(document[:index], document[index+1:]...)
		}
	}
	return document
}

// getKeyValue gets the value of keyName in document
func getKeyValue(keyName string, document *bson.D) interface{} {
	for _, key := range *document {
		if key.Name == keyName {
			return key.Value
		}
	}
	return nil
}

// setNestedValue takes a nested field - in the form "a.b.c" -
// its associated value, and a document. It then assigns that
// value to the appropriate nested field within the document
func setNestedValue(key string, value interface{}, document *bson.D) {
	index := strings.Index(key, ".")
	if index == -1 {
		*document = append(*document, bson.DocElem{key, value})
		return
	}
	keyName := key[0:index]
	subDocument := &bson.D{}
	elem := getKeyValue(keyName, document)
	var existingKey bool
	if elem != nil {
		subDocument = elem.(*bson.D)
		existingKey = true
	}
	setNestedValue(key[index+1:], value, subDocument)
	if !existingKey {
		*document = append(*document, bson.DocElem{keyName, subDocument})
	}
}

// validateHeaders takes an InputReader, and does some validation on the
// header fields. It returns an error if an issue is found in the header list
func validateHeaders(inputReader InputReader, hasHeaderLine bool) (validatedFields []string, err error) {
	unsortedHeaders := []string{}
	if hasHeaderLine {
		unsortedHeaders, err = inputReader.ReadHeadersFromSource()
		if err != nil {
			return nil, err
		}
	} else {
		unsortedHeaders = inputReader.GetHeaders()
	}

	headers := make([]string, len(unsortedHeaders), len(unsortedHeaders))
	copy(headers, unsortedHeaders)
	sort.Sort(sort.StringSlice(headers))

	for index, header := range headers {
		if strings.HasSuffix(header, ".") || strings.HasPrefix(header, ".") {
			return nil, fmt.Errorf("header '%v' can not start or end in '.'", header)
		}
		if strings.Contains(header, "..") {
			return nil, fmt.Errorf("header '%v' can not contain consecutive '.' characters", header)
		}
		// NOTE: since headers is sorted, this check ensures that no header
		// is incompatible with another one that occurs further down the list.
		// meant to prevent cases where we have headers like "a" and "a.c"
		for _, latterHeader := range headers[index+1:] {
			// NOTE: this means we will not support imports that have fields that
			// include e.g. a, a.b
			if strings.HasPrefix(latterHeader, header+".") {
				return nil, fmt.Errorf("incompatible headers found: '%v' and '%v",
					header, latterHeader)
			}
			// NOTE: this means we will not support imports that have fields like
			// a, a - since this is invalid in MongoDB
			if header == latterHeader {
				return nil, fmt.Errorf("headers can not be identical: '%v' and '%v",
					header, latterHeader)
			}
		}
		validatedFields = append(validatedFields, unsortedHeaders[index])
	}
	if len(headers) == 1 {
		log.Logf(1, "using field: %v", validatedFields[0])
	} else {
		log.Logf(1, "using fields: %v", strings.Join(validatedFields, ","))
	}
	return validatedFields, nil
}
