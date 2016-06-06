package de.otto.elasticsearch.client;

import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class RequestBuilderUtilTest {

    @Test
    public void shouldBuildUrl(){
        assertThat(RequestBuilderUtil.buildUrl("someHost:9200", "someIndexName"), is("http://someHost:9200/someIndexName"));
    }

    @Test
    public void shouldBuildUrlWithDocumentType(){
        assertThat(RequestBuilderUtil.buildUrl("someHost:9200", "someIndexName", "someDocumentType"), is("http://someHost:9200/someIndexName/someDocumentType"));
    }

    @Test
    public void shouldBuildUrlWithDocumentTypeAndOperation(){
        assertThat(RequestBuilderUtil.buildUrl("someHost:9200", "someIndexName", "someDocumentType", "someOperation"), is("http://someHost:9200/someIndexName/someDocumentType/someOperation"));
    }

    @Test
    public void shouldBuildUrlWithIndicesAndTypesAndOperation(){
        assertThat(RequestBuilderUtil.buildUrl("someHost:9200", new String[]{"someIndexName", "someIndexName2"}, new String[]{"someType", "someType2"}, "someOperation"), is("http://someHost:9200/someIndexName,someIndexName2/someType,someType2/someOperation"));
    }
}