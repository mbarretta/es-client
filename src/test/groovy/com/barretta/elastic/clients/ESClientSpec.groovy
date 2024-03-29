package com.barretta.elastic.clients

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.flush.FlushRequest
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.index.query.MatchAllQueryBuilder
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.index.query.TermQueryBuilder
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder
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
        def indexRequest = new IndexRequest(properties.esclient.index)
            .source([field1: "value1", field2: 2])
            .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
        esClient = new ESClient(
            new ESClient.Config(user: properties.esclient.user, pass: properties.esclient.pass, url: properties.esclient.url, index: properties.esclient.index)
        )
        esClient.deprecatedClient.index(indexRequest, RequestOptions.DEFAULT)
        esClient.indices().refresh()
    }

    def cleanupSpec() {
        esClient.deprecatedClient.indices().delete(new DeleteIndexRequest(properties.esclient.index), RequestOptions.DEFAULT)
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
        esClient.scrollQuery(query, 10, mapFunction)

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
        def data = [
            (ESClient.BulkOps.INSERT): [
                [bulkTest: "a"],
                [bulkTest: "b"]
            ]
        ]
        def search = new SearchRequest(indices: [properties.esclient.index]).source(
            new SearchSourceBuilder().query(QueryBuilders.termsQuery("bulkTest.keyword", "a", "b"))
        )

        when:
        esClient.bulk(data)
        Thread.sleep(1000)
        esClient.deprecatedClient.indices().flush(new FlushRequest(properties.esclient.index), RequestOptions.DEFAULT)

        then:
        esClient.deprecatedClient.search(search, RequestOptions.DEFAULT).hits.totalHits.value == 2
    }

    def "bulk insert works when source docs contain an _id"() {
        setup:
        def data = [
            (ESClient.BulkOps.INSERT): [
                [bulkTest: "c", "_id": "1"],
                [bulkTest: "d", "_id": "2"]
            ]
        ]
        def search = new SearchRequest(indices: [properties.esclient.index])
            .source(new SearchSourceBuilder().query(QueryBuilders.termsQuery("bulkTest.keyword", "c", "d")))

        when:
        esClient.bulk(data)
        Thread.sleep(1000)
        esClient.deprecatedClient.indices().flush(new FlushRequest(properties.esclient.index), RequestOptions.DEFAULT)

        then:
        esClient.deprecatedClient.search(search, RequestOptions.DEFAULT).hits.totalHits.value == 2
        esClient.deprecatedClient.get(new GetRequest(properties.esclient.index, "1"), RequestOptions.DEFAULT).isExists()
    }

    def "bulk update works when source docs contain an _index"() {
        setup:
        def data = [
            [bulkTest: "a", "_id": 1],
            [bulkTest: "b", "_id": 2]
        ]
        esClient.bulkInsert(data, "index-test-index")
        Thread.sleep(1000)
        esClient.deprecatedClient.indices().flush(new FlushRequest("index-test-index"), RequestOptions.DEFAULT)

        when:
        data = [
            [bulkTest: "c", "_id": 1, "_index": "index-test-index"],
            [bulkTest: "d", "_id": 2, "_index": "index-test-index"]
        ]
        esClient.bulk([(ESClient.BulkOps.UPDATE): data], properties.esClient.index as String)
        Thread.sleep(1000)
        esClient.deprecatedClient.indices().flush(new FlushRequest("index-test-index"), RequestOptions.DEFAULT)

        then:
        def search = new SearchRequest(indices: ["index-test-index"])
            .source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()))

        esClient.deprecatedClient.search(search, RequestOptions.DEFAULT).hits.totalHits.value == 2
        esClient.deprecatedClient.get(new GetRequest("index-test-index", "1"), RequestOptions.DEFAULT).source.bulkTest == "c"

        cleanup:
        esClient.deprecatedClient.indices().delete(new DeleteIndexRequest("index-test-index"), RequestOptions.DEFAULT)
    }

    def "term query works"() {
        when:
        def response = esClient.termQuery("field1", "value1")

        then:
        response.hits.totalHits.value == 1l
    }

    def "existsByMatch works"() {
        expect:
        esClient.existsByMatch("field1", "value1")
        !esClient.existsByMatch("foo", "bar")
    }

    def "getByMatch works"() {
        when:
        def response = esClient.getByMatch("field1", "value1")

        then:
        response.hits.totalHits.value == 1l
    }

    def "rawRequest works"() {
        when:
        def response = esClient.rawRequest("GET", "/${properties.esclient.index}/_search", [query: [match: [field1: "value1"]]])

        then:
        response != null
        response.hits.total.value == 1
    }

    def "compositeAggs works"() {
        setup:
        def data = [[comptest: 20, comptestterm: "skipme"]]
        20.times {
            data << [comptest: it, comptestterm: "term"]
        }
        esClient.bulk([(ESClient.BulkOps.INSERT): data])
        Thread.sleep(1000)
        esClient.deprecatedClient.indices().flush(new FlushRequest(properties.esclient.index), RequestOptions.DEFAULT)
        def sources = [
            new TermsValuesSourceBuilder("terms").field("comptest")
        ]
        def query = QueryBuilders.constantScoreQuery(new TermQueryBuilder("comptestterm.keyword", "term"))
        def compositeAgg = AggregationBuilders.composite("composite", sources).size(5)

        when:
        def buckets = esClient.compositeAgg(compositeAgg, query)

        then:
        buckets.size() == 20
    }
}
