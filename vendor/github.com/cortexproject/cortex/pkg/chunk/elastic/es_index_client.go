package elastic

import (
	"context"
	"flag"
	"github.com/cortexproject/cortex/pkg/chunk"
	chunk_util "github.com/cortexproject/cortex/pkg/chunk/util"
	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"

	"reflect"

	"fmt"
	"github.com/olivere/elastic"
)

const (
	null           = string('\xff')
	max_fetch_docs = 1000
)

// BoltDBConfig for a BoltDB index client.
type ElasticConfig struct {
	Address 	string 	`yaml:"address"`
	IndexType 	string 	`yaml:"index_type"`
}

// RegisterFlags registers flags.
func (cfg *ElasticConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.Address, "elastic.address", "http://127.0.0.1:9200", "Address of ElasticSearch.")
	f.StringVar(&cfg.IndexType, "elastic.index_type", "lokiindex", "Index Type used in ElasticSearch.")
}

// Tweet is a structure used for serializing/deserializing data in Elasticsearch.
type LokiIndex struct {
	Hash    string		`json:"hash"`
	Range  	string		`json:"range"`
	Value   string		`json:"value,omitempty"`
}

const mapping = `
{
	"settings":{
		"number_of_shards": 1,
		"number_of_replicas": 0
	},
	"mappings":{
		"lokiindex":{
			"properties":{
				"hash":{
					"type":"keyword"
				},
				"range":{
					"type":"keyword"
				},
				"value":{
					"type":"keyword"
				}
			}
		}
	}
}`

var client *elastic.Client
// Starting with elastic.v5, you must pass a context to execute each service
var ctx = context.Background()

// StorageClient implements chunk.IndexClient and chunk.ObjectClient for Cassandra.
type esClient struct {
	cfg 		ElasticConfig
	client   	*elastic.Client
}

func (e *esClient) Stop() {
	e.client.Stop()
}

// ES batching isn't really useful in this case, its more to do multiple
// atomic writes.  Therefore we just do a bunch of writes in parallel.
type writeBatch struct {
	entries []chunk.IndexEntry
}

func (e *esClient) NewWriteBatch() chunk.WriteBatch {
	return &writeBatch{}
}

func (b *writeBatch) Add(tableName, hashValue string, rangeValue []byte, value []byte) {
	b.entries = append(b.entries, chunk.IndexEntry{
		TableName:  tableName,
		HashValue:  hashValue,
		RangeValue: rangeValue,
		Value:      value,
	})
}

func (e *esClient) BatchWrite(ctx context.Context, batch chunk.WriteBatch) error {
	b := batch.(*writeBatch)

	indexName := b.entries[0].TableName

	exists, err := e.client.IndexExists(indexName).Do(ctx)
	if err != nil {
		// Handle error
		level.Error(util.Logger).Log("msg", fmt.Sprintf("IndexName %s exists check has error!", indexName))
		panic(err)
	}
	if !exists {
		// Create a new index.
		createIndex, err := e.client.CreateIndex(indexName).BodyString(mapping).Do(ctx)
		if err != nil {
			level.Error(util.Logger).Log("msg", fmt.Sprintf("Create IndexName %s failed!", indexName))
			panic(err)
		}
		if !createIndex.Acknowledged {
			// Not acknowledged
		}
	}

	bulkRequest := e.client.Bulk()
	for _, entry := range b.entries {
		loki := LokiIndex{Hash: entry.HashValue, Range: string(entry.RangeValue), Value: string(entry.Value)}
		req := elastic.NewBulkIndexRequest().Index(indexName).Type(e.cfg.IndexType).Doc(loki)
		bulkRequest = bulkRequest.Add(req)
	}

	bulkResponse, err := bulkRequest.Do(ctx)
	if err != nil {
		return err
	}

	if bulkResponse != nil {

	}
	return nil
}

// readBatch represents a batch of rows read from ElasticSearch.
type readBatch struct {
	rangeValue []byte
	value      []byte
}

func (r readBatch) Iterator() chunk.ReadBatchIterator {
	return &elasticReadBatchIterator{
		readBatch: r,
	}
}

type elasticReadBatchIterator struct {
	consumed bool
	readBatch
}

func (r *elasticReadBatchIterator) Next() bool {
	if r.consumed {
		return false
	}
	r.consumed = true
	return true
}

func (r *elasticReadBatchIterator) RangeValue() []byte {
	return r.rangeValue
}

func (r *elasticReadBatchIterator) Value() []byte {
	return r.value
}

func (e *esClient) QueryPages(ctx context.Context, queries []chunk.IndexQuery, callback func(chunk.IndexQuery, chunk.ReadBatch) (shouldContinue bool)) error {
	return chunk_util.DoParallelQueries(ctx, e.query, queries, callback)
}

func (e *esClient) query(ctx context.Context, query chunk.IndexQuery, callback func(chunk.ReadBatch) (shouldContinue bool)) error {
	var rangeQuery *elastic.RangeQuery
	var valueTermQuery *elastic.TermQuery

	level.Info(util.Logger).Log("msg", fmt.Sprintf(
		"tableName [%s] hash [%s], rangeValuePrefix [%s], rangeValueStart [%s]", query.TableName, query.HashValue, query.RangeValuePrefix, query.RangeValueStart))
	hashTermQuery := elastic.NewTermQuery("hash", query.HashValue)
	switch {
	case len(query.RangeValuePrefix) > 0 && query.ValueEqual == nil:
		rangeQuery = elastic.NewRangeQuery("range").Gte(string(query.RangeValuePrefix)).
			Lt(string(query.RangeValuePrefix) + null)

	case len(query.RangeValuePrefix) > 0 && query.ValueEqual != nil:
		rangeQuery = elastic.NewRangeQuery("range").Gte(string(query.RangeValuePrefix)).
			Lt(string(query.RangeValuePrefix) + null)
		valueTermQuery= elastic.NewTermQuery("value", query.ValueEqual)

	case len(query.RangeValueStart) > 0 && query.ValueEqual == nil:
		rangeQuery = elastic.NewRangeQuery("range").Gte(string(query.RangeValueStart))

	case len(query.RangeValueStart) > 0 && query.ValueEqual != nil:
		rangeQuery = elastic.NewRangeQuery("range").Gte(string(query.RangeValueStart))
		valueTermQuery= elastic.NewTermQuery("value", query.ValueEqual)

	case query.ValueEqual != nil:
		valueTermQuery= elastic.NewTermQuery("value", query.ValueEqual)

	case query.ValueEqual == nil:
		break
	}

	exists, err := e.client.IndexExists(query.TableName).Do(ctx)
	if err != nil {
		// Handle error
		fmt.Println(exists)
	}

	// Search with a term query
	baseQuery := e.client.Search().
		Index(query.TableName).   // search in index"
		Query(hashTermQuery)
	if valueTermQuery != nil {
		baseQuery = baseQuery.Query(valueTermQuery)
	}
	if rangeQuery != nil {
		baseQuery = baseQuery.Query(rangeQuery)
	}

	// Search with a term query
	searchResult, err := baseQuery.
		//Sort("range", true). // sort by "range" field, ascending
		From(0).Size(max_fetch_docs).   // take documents 0-9
		Pretty(true).       // pretty print request and response JSON
		Do(ctx) // execute

	if searchResult == nil || searchResult.Hits == nil {
		return nil
	}

	if err != nil {
		// Handle error
		level.Error(util.Logger).Log("msg", fmt.Sprintf("Query in index %s met error!", query.TableName))
		panic(err)
	}

	// searchResult is of type SearchResult and returns hits, suggestions,
	// and all kinds of other information from Elasticsearch.
	level.Debug(util.Logger).Log("msg", fmt.Sprintf("Query took %d milliseconds with result num\n", searchResult.TookInMillis))

	var batch readBatch
	var ttyp LokiIndex
	for _, item := range searchResult.Each(reflect.TypeOf(ttyp)) {
		if t, ok := item.(LokiIndex); ok {
			level.Debug(util.Logger).Log("msg", fmt.Sprintf("LokiIndex by hash %s: range %s, value %s\n", t.Hash, t.Range, t.Value))
			batch.rangeValue = []byte(t.Range)
			batch.value = []byte(t.Value)

			if !callback(&batch) {
				return nil
			}
		}
	}
	// TotalHits is another convenience function that works even when something goes wrong.
	//fmt.Printf("Found a total of %d tweets\n", searchResult.TotalHits())

	return nil
}

func NewESIndexClient(cfg ElasticConfig) (chunk.IndexClient, error){
	client, err := newES(cfg)
	if err != nil {
		return nil, err
	}
	indexClient := &esClient{
		cfg,
		client,
	}
	return indexClient, nil
}

func newES(cfg ElasticConfig) (*elastic.Client, error) {
	// Obtain a client and connect to the default Elasticsearch installation
	// on 127.0.0.1:9200. Of course you can configure your client to connect
	// to other hosts and configure it in various other ways.
	var err error
	client, err = elastic.NewClient(elastic.SetURL(cfg.Address), elastic.SetSniff(false))
	if err != nil {
		return nil, err
	}

	return client, nil
}