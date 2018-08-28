package com.elastic.barretta.clients

import groovy.util.logging.Slf4j
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
        SearchRequest searchRequest = new SearchRequest(config.index)
        searchRequest.scroll(scroll)
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
        searchSourceBuilder.size(batchSize)
        searchSourceBuilder.query(query)
        searchRequest.source(searchSourceBuilder)

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT)
        String scrollId = searchResponse.getScrollId()
        SearchHit[] searchHits = searchResponse.getHits().getHits()

        log.info("found [${searchHits.size()}] hits")
        while (searchHits != null && searchHits.length > 0) {
            searchHits.each {
                mapFunction(it)
            }
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId)
            scrollRequest.scroll(scroll)
            searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT)
            scrollId = searchResponse.getScrollId()
            searchHits = searchResponse.getHits().getHits()
        }

        ClearScrollRequest clearScrollRequest = new ClearScrollRequest()
        clearScrollRequest.addScrollId(scrollId)
        ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest)
        return clearScrollResponse.isSucceeded()
    }

    def getIndicesFromPattern(String indexPattern) {
        def request = new GetAliasesRequest().indices(indexPattern)
        request.indicesOptions(IndicesOptions.STRICT_EXPAND_OPEN)
        return client.indices().getAlias(request, RequestOptions.DEFAULT)
    }

    def getIndex(String indexName) {
        GetIndexRequest request = new GetIndexRequest().indices(indexName)
        request.includeDefaults(true)
        return client.indices().get(request, RequestOptions.DEFAULT)
    }
}

