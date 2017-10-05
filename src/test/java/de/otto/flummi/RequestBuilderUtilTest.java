package de.otto.flummi;

import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class RequestBuilderUtilTest {

    @Test
    public void shouldBuildUrlWithDocumentType() {
        assertThat(RequestBuilderUtil.buildUrl("someIndexName", "someDocumentType"), is("/someIndexName/someDocumentType"));
    }

    @Test
    public void shouldBuildUrlWithDocumentTypeAndOperation() {
        assertThat(RequestBuilderUtil.buildUrl("someIndexName", "someDocumentType", "someOperation"), is("/someIndexName/someDocumentType/someOperation"));
    }

    @Test
    public void shouldBuildUrlWithIndicesAndTypesAndOperation() {
        assertThat(RequestBuilderUtil.buildUrl(new String[]{"someIndexName", "someIndexName2"}, new String[]{"someType", "someType2"}, "someOperation"), is("/someIndexName,someIndexName2/someType,someType2/someOperation"));
    }
}