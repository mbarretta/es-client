package com.elastic.barretta.clients

import groovy.util.logging.Slf4j
import groovyx.gpars.GParsPool
import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.client.CredentialsProvider
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest
import org.elasticsearch.action.admin.indices.get.GetIndexRequest
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest
import org.elasticsearch.action.search.*
import org.elasticsearch.action.support.IndicesOptions
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

import java.util.concurrent.atomic.AtomicInteger

@Slf4j
class ESClient {
    @Delegate
    RestHighLevelClient client
    Config config

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
        return client.ping()
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

    def scrollQuery(QueryBuilder query, int batchSize = 100, Closure mapFunction = {}) {
        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L))
        final def slices = 5

        GParsPool.withPool(slices) {
            def sliceHandler = { slice ->
                def sliceBuilder = new SliceBuilder(slice, slices)
                SearchRequest searchRequest = new SearchRequest(config.index)
                searchRequest.scroll(scroll)
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                searchSourceBuilder.size(batchSize)
                searchSourceBuilder.slice(sliceBuilder)
                searchSourceBuilder.query(query)
                searchRequest.source(searchSourceBuilder)
                def searchResponse = client.search(searchRequest, RequestOptions.DEFAULT)

                String scrollId = searchResponse.scrollId
                SearchHit[] searchHits = searchResponse.hits.hits

                log.info("in slice [$slice] have [${searchHits.size()}] hits of [${searchResponse.hits.totalHits}]")
                while (searchHits != null && searchHits.length > 0) {
                    searchHits.each {
                        mapFunction(it)
                    }
                    SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId)
                    scrollRequest.scroll(scroll)
                    searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT)
                    scrollId = searchResponse.scrollId
                    searchHits = searchResponse.hits.hits
                }
                ClearScrollRequest clearScrollRequest = new ClearScrollRequest()
                clearScrollRequest.addScrollId(scrollId)
                client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT)
            }.asyncFun()

            slices.times {
                sliceHandler(it).get()
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
}

