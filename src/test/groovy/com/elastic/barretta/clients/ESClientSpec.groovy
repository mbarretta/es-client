package com.elastic.barretta.clients

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.flush.FlushRequest
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.builder.SearchSourceBuilder
import spock.lang.Shared
import spock.lang.Specification

import java.nio.file.Files

class ESClientSpec extends Specification {
    @Shared
    def properties = new ConfigSlurper().parse(this.class.classLoader.getResource("test_properties.groovy"))

    @Shared
    ESClient esClient

    def setupSpec() {
        def indexRequest = new IndexRequest(properties.esclient.index, "_doc")
            .source([field1: "value1", field2: 2])
            .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
        esClient = new ESClient(
            new ESClient.Config(user: properties.esclient.user, pass: properties.esclient.pass, url: properties.esclient.url, index: properties.esclient.index)
        )
        esClient.index(indexRequest, RequestOptions.DEFAULT).forcedRefresh()
    }

    def cleanupSpec() {
        esClient.indices().delete(new DeleteIndexRequest(properties.esclient.index), RequestOptions.DEFAULT)
    }

    def "ScrollQuery will run mapFunction"() {
        setup:
        def tempFile = Files.createTempFile(null, null).toFile()
        def mapFunction = {
            new FileWriter(tempFile).withWriter { writer ->
                writer << it
            }
        }
        def query = QueryBuilders.queryStringQuery("*")

        when:
        esClient.scrollQuery(query, 10, 2, mapFunction)

        then:
        tempFile.text.length() > 0
    }

    def "getIndicesFromPattern works"() {
        when:
        def response = esClient.getIndicesFromPattern(properties.esclient.index + "*")

        then:
        response.aliases.size() == 1
    }

    def "getIndex works"() {
        expect:
        esClient.getIndex(properties.esclient.index).toString().length() > 0
    }

    def "bulk Insert works"() {
        setup:
        def data = [(ESClient.BulkOps.INSERT): [
            [bulkTest: "a"],
            [bulkTest: "b"]
        ]
        ]
        def search = new SearchRequest(indices: [properties.esclient.index])
        def source = new SearchSourceBuilder()
        source.query(QueryBuilders.termsQuery("bulkTest.keyword", "a", "b"))
        search.source(source)

        when:
        esClient.bulk(data)
        esClient.indices().flush(new FlushRequest(properties.esclient.index), RequestOptions.DEFAULT)

        then:
        esClient.search(search, RequestOptions.DEFAULT).hits.totalHits == 2
    }

    def "bulk insert works when source docs contain an _id"() {
        setup:
        def data = [(ESClient.BulkOps.INSERT): [
            [bulkTest: "a", "_id":"1"],
            [bulkTest: "b", "_id":"2"]
        ]
        ]
        def search = new SearchRequest(indices: [properties.esclient.index])
        def source = new SearchSourceBuilder()
        source.query(QueryBuilders.termsQuery("bulkTest.keyword", "a", "b"))
        search.source(source)

        when:
        esClient.bulk(data)
        esClient.indices().flush(new FlushRequest(properties.esclient.index), RequestOptions.DEFAULT)

        then:
        esClient.search(search, RequestOptions.DEFAULT).hits.totalHits == 2
        esClient.get(new GetRequest(properties.esclient.index, "_doc", "1"), RequestOptions.DEFAULT).isExists()
    }
}
