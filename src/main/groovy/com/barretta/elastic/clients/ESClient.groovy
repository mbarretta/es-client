package com.barretta.elastic.clients

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j
import groovyx.gpars.GParsPool
import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.client.CredentialsProvider
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.elasticsearch.action.ActionListener
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest
import org.elasticsearch.action.bulk.BulkItemResponse
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.ClearScrollRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchScrollRequest
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.*
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.MatchQueryBuilder
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.Scroll
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.slice.SliceBuilder

@Slf4j
class ESClient {
    @Delegate
    RestHighLevelClient client
    Config config
    static enum BulkOps {
        INSERT, CREATE, UPDATE, DELETE
    }

    static class Config {
        String url
        String user
        String pass
        String index
    }

    ESClient(Config config) {
        this.config = config
        init()
    }

    def test() {
        return client.ping(RequestOptions.DEFAULT)
    }

    private init() {
        assert config != null, "ESClient is not configured: use ESClient(Config config) method to instantiate and put shit in it"
        def url = new URL(config.url)
        def builder = RestClient.builder(new HttpHost(url.host, url.port, url.protocol))
        if (config.user) {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider()
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(config.user, config.pass))

            builder.setHttpClientConfigCallback(
                new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                    }
                }
            )
        }

        client = new RestHighLevelClient(builder)
    }

    def scrollQuery(QueryBuilder query, int batchSize, Closure mapFunction) {
        scrollQuery(query, batchSize, 5, 1, mapFunction)
    }

    def scrollQuery(QueryBuilder query, int batchSize, int slices, int minutes, Closure mapFunction) {
        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(minutes as long))

        def sliceHandler = { slice ->
            def sliceBuilder = new SliceBuilder(slice, slices)
            def searchSourceBuilder = new SearchSourceBuilder().size(batchSize).slice(sliceBuilder).query(query)
            def searchRequest = new SearchRequest(config.index).scroll(scroll).source(searchSourceBuilder)
            def searchResponse = client.search(searchRequest, RequestOptions.DEFAULT)

            String scrollId = searchResponse.scrollId
            SearchHit[] searchHits = searchResponse.hits.hits

            log.trace("in slice [$slice] with [${searchResponse.hits.totalHits}] total hits")
            while (searchHits != null && searchHits.length > 0) {
                log.debug("working [${searchHits.length}] hits in slice [$slice] and scroll [$scrollId]")
                searchHits.each {
                    mapFunction(it)
                }
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId).scroll(scroll)
                searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT)
                scrollId = searchResponse.scrollId
                searchHits = searchResponse.hits.hits
            }
            log.trace("...done with slice [$slice]")
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest()
            clearScrollRequest.addScrollId(scrollId)
            client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT)
        }

        GParsPool.withPool {
            (0..slices - 1).eachParallel {
                sliceHandler(it)
            }
        }

    }

    def getIndicesFromPattern(String indexPattern) {
        def request = new GetAliasesRequest().indices(indexPattern)
        request.indicesOptions(IndicesOptions.STRICT_EXPAND_OPEN)
        return client.indices().getAlias(request, RequestOptions.DEFAULT)
    }

    def getIndex(String indexName) {
        GetIndexRequest request = new GetIndexRequest(indexName)
        return client.indices().get(request, RequestOptions.DEFAULT)
    }

    def bulkInsert(List<Map> records, String index = config.index) {
        return bulk([(BulkOps.INSERT): records], 500, index)
    }

    def bulk(Map<BulkOps, List<Map>> records, String index = config.index) {
        bulk(records, 500, index)
    }

    //todo: error handling - do better
    def bulk(Map<BulkOps, List<Map>> records, int size, String index = config.index) {

        def request = new BulkRequest().timeout(TimeValue.timeValueSeconds(5L)).setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)

        try {
            records[BulkOps.INSERT].each {
                if (it) {
                    if (it._id) {
                        def id = it.remove("_id") as String
                        request.add(new IndexRequest(index).id(id).source(it))
                    } else {
                        request.add(new IndexRequest(index).source(it))
                    }
                }
            }
            records[BulkOps.CREATE].each {
                if (it) {
                    if (it._id) {
                        def id = it.remove("_id") as String
                        request.add(new IndexRequest(index).id(id).opType(DocWriteRequest.OpType.CREATE).source(it))
                    } else {
                        request.add(new IndexRequest(index).opType(DocWriteRequest.OpType.CREATE).source(it))
                    }
                }
            }
            records[BulkOps.UPDATE].each {
                if (it) {
                    def id = it.remove("_id") as String
                    request.add(new UpdateRequest(index, id).doc(it))
                }
            }
            records[BulkOps.DELETE].each {
                if (it) {
                    request.add(new DeleteRequest(index, it._id as String))
                }
            }

            client.bulkAsync(request, RequestOptions.DEFAULT, new ActionListener<BulkResponse>() {
                @Override
                void onResponse(BulkResponse bulkItemResponses) {
                    if (bulkItemResponses.hasFailures()) {
                        bulkItemResponses.findAll { it.failed }.each { BulkItemResponse response ->
                            log.error("bulk error: ${response.failureMessage}")
                        }
                    }
                }

                @Override
                void onFailure(Exception e) {
                    log.error("BULK ERROR: [$e.message] [$e.cause]", e)
                }
            })

        } catch (e) {
            log.error("uh oh", e)
        }
    }

    def termQuery(String field, value, String index = config.index) {
        return client.search(new SearchRequest(index).source(new SearchSourceBuilder().query(QueryBuilders.termQuery(field, value))), RequestOptions.DEFAULT)
    }

    def index(Map doc, String index = config.index) {
        def request = new IndexRequest(index, "_doc")
        if (doc._id) {
            request.id(doc._id as String)
        }
        request.source(doc)
        return client.index(request, RequestOptions.DEFAULT).id
    }

    def update(Map doc, String index = config.index) {
        if (doc.containsKey("_id")) {
            def request = new UpdateRequest(index, doc.remove("_id") as String)
            request.doc(doc)
            return client.update(request, RequestOptions.DEFAULT)
        } else {
            log.error("missing _id: can't update")
            return null
        }
    }

    def existsByMatch(String field, String value, String index = config.index) {
        def request = new SearchRequest(index)
        def search = new SearchSourceBuilder().size(1).fetchSource(false)
        def match = new MatchQueryBuilder(field, value)
        search.query(match)
        request.source(search)
        def response = client.search(request, RequestOptions.DEFAULT)
        return response.hits.totalHits.value > 0
    }

    def getByMatch(String field, String value, String index = config.index) {
        def request = new SearchRequest(index)
        def search = new SearchSourceBuilder().size(1).fetchSource(false)
        def match = new MatchQueryBuilder(field, value)
        search.query(match)
        request.source(search)
        def response = client.search(request, RequestOptions.DEFAULT)

        return response.hits.totalHits.value > 0 ? response : null
    }

    def rawRequest(String method, String endpoint, Map doc) {
        def request = new Request(method, endpoint)
        request.setJsonEntity(JsonOutput.toJson(doc))
        def response = client.lowLevelClient.performRequest(request)
        return new JsonSlurper().parse(response.entity.content)
    }

    def compositeAgg(CompositeAggregationBuilder compositeAggregationBuilder, QueryBuilder filter, String index = config.index) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
        searchSourceBuilder.aggregation(compositeAggregationBuilder)
        searchSourceBuilder.query(filter)
        searchSourceBuilder.size(0)

        SearchRequest searchRequest = new SearchRequest()
        searchRequest.source(searchSourceBuilder)
        searchRequest.indices(index)

        def results = []
        def afterKey = [:]
        while (results.isEmpty() || afterKey != null) {
            def searchResults = client.search(searchRequest, RequestOptions.DEFAULT)
            def agg = searchResults.aggregations.get("composite")
            results += agg.buckets
            afterKey = agg.afterKey()

            compositeAggregationBuilder = compositeAggregationBuilder.aggregateAfter(afterKey)
        }
        return results
    }
}

