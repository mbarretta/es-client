package com.elastic.barretta.clients

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
        esClient = new ESClient(new ESClient.Config(user: properties.esclient.user, pass: properties.esclient.pass, url: properties.esclient.url, index: properties.esclient.index))
    }

    def "ScrollQuery will run mapFunction"() {
        setup:
        def tempFile = Files.createTempFile(null, null).toFile()
        def tempFileWriter = new FileWriter(tempFile)
        def mapFunction = {
            tempFileWriter.withWriter { writer ->
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
}
