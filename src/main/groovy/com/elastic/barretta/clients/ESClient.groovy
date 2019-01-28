package com.elastic.barretta.clients

import groovy.util.logging.Slf4j
import groovyx.gpars.GParsPool
import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.client.CredentialsProvider
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.elasticsearch.action.DocWriteRequest
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest
import org.elasticsearch.action.admin.indices.get.GetIndexRequest
import org.elasticsearch.action.bulk.BulkProcessor
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.ClearScrollRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchScrollRequest
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestClientBuilder
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.search.Scroll
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.slice.SliceBuilder

import java.util.concurrent.TimeUnit

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
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider()
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(config.user, config.pass))

        def builder = RestClient.builder(new HttpHost(url.host, url.port, url.protocol))
            .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
            }
        })
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

            log.info("in slice [$slice] with [${searchResponse.hits.totalHits}] total hits")
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
            log.info("...done with slice [$slice]")
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
        GetIndexRequest request = new GetIndexRequest().indices(indexName)
        return client.indices().get(request, RequestOptions.DEFAULT)
    }

    def bulkInsert(List<Map> records, String index = config.index) {
        return bulk([(BulkOps.INSERT): records], index)
    }

    //todo: error handling - do better
    def bulk(Map<BulkOps, List<Map>> records, String index = config.index) {

        init()

        def listener = new BulkProcessor.Listener() {

            @Override
            void beforeBulk(long executionId, BulkRequest request) {
                log.info("bulk-ing [${request.numberOfActions()}] records to [$index]")
            }

            @Override
            void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                log.info("successfully bulked [$response.items.length]")
            }

            @Override
            void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                log.error("error running bulk insert [$failure.message]", failure)

            }
        }
        def builder = BulkProcessor.builder(client.&bulkAsync, listener).setFlushInterval(TimeValue.timeValueSeconds(5L)).build()

        records[BulkOps.INSERT].each {
            builder.add(new IndexRequest(index, "_doc").source(it))
        }
        records[BulkOps.CREATE].each {
            builder.add(new IndexRequest(index, "_doc").opType(DocWriteRequest.OpType.CREATE).source(it))
        }
        records[BulkOps.UPDATE].each {
            builder.add(new UpdateRequest(index, "_doc", it._id).doc(it))
        }
        records[BulkOps.DELETE].each {
            def id = it.containsKey('_id') ? it._id : it.id
            builder.add(new DeleteRequest(index, "_doc", id))
        }

        builder.awaitClose(5l, TimeUnit.SECONDS)
    }
}

