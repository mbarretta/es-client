package com.elastic.barretta.clients

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.index.query.QueryBuilders
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
}
