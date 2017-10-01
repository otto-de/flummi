package de.otto.flummi.request;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import de.otto.flummi.CompletedFuture;
import de.otto.flummi.MockResponse;
import de.otto.flummi.response.MultiGetResponse;
import de.otto.flummi.response.MultiGetResponseDocument;
import de.otto.flummi.util.HttpClientWrapper;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static de.otto.flummi.request.GsonHelper.object;
import static de.otto.flummi.response.MultiGetRequestDocument.multiGetRequestDocumentBuilder;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

import java.nio.charset.Charset;

import org.asynchttpclient.BoundRequestBuilder;

public class MultiGetRequestBuilderTest {

    private HttpClientWrapper httpClient;
    private MultiGetRequestBuilder requestBuilder;

    public static final String ONE_DOC_FOUND_RESPONSE = "{\n" +
            "  \"docs\": [\n" +
            "    {\n" +
            "      \"_index\": \"dynppoc_variations_1465472184\",\n" +
            "      \"_type\": \"variation\",\n" +
            "      \"_id\": \"V1\",\n" +
            "      \"_version\": 1,\n" +
            "      \"found\": true,\n" +
            "      \"_source\": {\n" +
            "        \"variationId\": \"V1\",\n" +
            "        \"name\": \"name1\"\n" +
            "      }\n" +
            "    }\n" +
            "  ]\n" +
            "}";

    public static final String TWO_DOC_FOUND_RESPONSE = "{\n" +
            "  \"docs\": [\n" +
            "    {\n" +
            "      \"_index\": \"dynppoc_variations_1465472184\",\n" +
            "      \"_type\": \"variation\",\n" +
            "      \"_id\": \"V1\",\n" +
            "      \"_version\": 1,\n" +
            "      \"found\": true,\n" +
            "      \"_source\": {\n" +
            "        \"variationId\": \"V1\",\n" +
            "        \"name\": \"name1\"\n" +
            "      }\n" +
            "    },\n" +
            "    {\n" +
            "      \"_index\": \"dynppoc_variations_1465472184\",\n" +
            "      \"_type\": \"variation\",\n" +
            "      \"_id\": \"V2\",\n" +
            "      \"_version\": 1,\n" +
            "      \"found\": true,\n" +
            "      \"_source\": {\n" +
            "        \"variationId\": \"V2\",\n" +
            "        \"name\": \"name2\"\n" +
            "      }\n" +
            "    }\n" +
            "  ]\n" +
            "}";

    public static final String NOT_FOUND_RESPONSE = "{\n" +
            "  \"docs\": [\n" +
            "    {\n" +
            "      \"_index\": \"dynppoc_variations_1465472184\",\n" +
            "      \"_type\": \"variation\",\n" +
            "      \"_id\": \"V1\",\n" +
            "      \"_version\": 1,\n" +
            "      \"found\": false\n" +
            "    }\n" +
            "  ]\n" +
            "}";

    @BeforeMethod
    public void setUp() throws Exception {
        httpClient = mock(HttpClientWrapper.class);
        requestBuilder = new MultiGetRequestBuilder(httpClient, "some-index");
    }

    @Test
    public void shouldReturnOneMatchedDocument() throws Exception {
        // given
        BoundRequestBuilder boundRequestBuilderMock = mock(BoundRequestBuilder.class);
        when(httpClient.preparePost("/some-index/_mget")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBody(any(String.class))).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setCharset(Charset.forName("UTF-8"))).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture<>(new MockResponse(200, "ok", ONE_DOC_FOUND_RESPONSE)));

        // when
        MultiGetResponse response = requestBuilder.setRequestDocuments(asList(multiGetRequestDocumentBuilder().withId("V1").build())).execute();

        //then
        verify(boundRequestBuilderMock).setBody("{\"docs\":[{\"_id\":\"V1\"}]}");
        verify(httpClient).preparePost("/some-index/_mget");
        assertThat(response.getMultiGetResponseDocuments(), hasSize(1));
        assertThat(response.getMultiGetResponseDocuments().get(0), is(new MultiGetResponseDocument("V1", true, object("variationId", new JsonPrimitive("V1"), "name", new JsonPrimitive("name1")))));
    }

    @Test
    public void shouldReturnMultipleMatchedDocument() throws Exception {
        // given
        BoundRequestBuilder boundRequestBuilderMock = mock(BoundRequestBuilder.class);
        when(httpClient.preparePost("/some-index/_mget")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBody(any(String.class))).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setCharset(Charset.forName("UTF-8"))).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture<>(new MockResponse(200, "ok", TWO_DOC_FOUND_RESPONSE)));

        // when
        MultiGetResponse response = requestBuilder.setRequestDocuments(asList(multiGetRequestDocumentBuilder().withId("V1").build(), multiGetRequestDocumentBuilder().withId("V2").build())).execute();

        //then
        verify(boundRequestBuilderMock).setBody("{\"docs\":[{\"_id\":\"V1\"},{\"_id\":\"V2\"}]}");
        verify(httpClient).preparePost("/some-index/_mget");
        assertThat(response.getMultiGetResponseDocuments(), hasSize(2));
        assertThat(response.getMultiGetResponseDocuments().get(0), is(new MultiGetResponseDocument("V1", true, object("variationId", new JsonPrimitive("V1"), "name", new JsonPrimitive("name1")))));
        assertThat(response.getMultiGetResponseDocuments().get(1), is(new MultiGetResponseDocument("V2", true, object("variationId", new JsonPrimitive("V2"), "name", new JsonPrimitive("name2")))));
    }

    @Test
    public void shouldBuildAndReturnDocumentWithTypeAndIndex() throws Exception {
        // given
        BoundRequestBuilder boundRequestBuilderMock = mock(BoundRequestBuilder.class);
        when(httpClient.preparePost("/some-index/_mget")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBody(any(String.class))).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setCharset(Charset.forName("UTF-8"))).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture<>(new MockResponse(200, "ok", ONE_DOC_FOUND_RESPONSE)));

        // when
        MultiGetResponse response = requestBuilder.setRequestDocuments(asList(multiGetRequestDocumentBuilder().withId("V1").withIndex("I1").withType("T1").build())).execute();

        //then
        verify(boundRequestBuilderMock).setBody("{\"docs\":[{\"_id\":\"V1\",\"_type\":\"T1\",\"_index\":\"I1\"}]}");
    }

    @Test
    public void shouldReturnAnEmptyResultFor404Response() throws Exception {
        // given
        BoundRequestBuilder boundRequestBuilderMock = mock(BoundRequestBuilder.class);
        when(httpClient.preparePost("/some-index/_mget")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBody(any(String.class))).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setCharset(Charset.forName("UTF-8"))).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture<>(new MockResponse(404, "not found", "")));

        // when
        MultiGetResponse response = requestBuilder.setRequestDocuments(asList(multiGetRequestDocumentBuilder().withId("V1").withIndex("I1").withType("T1").build())).execute();

        //then
        assertThat(response.getMultiGetResponseDocuments(), is(empty()));
    }

    @Test
    public void shouldParseNotFoundDocument() throws Exception {
        // given
        BoundRequestBuilder boundRequestBuilderMock = mock(BoundRequestBuilder.class);
        when(httpClient.preparePost("/some-index/_mget")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBody(any(String.class))).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setCharset(Charset.forName("UTF-8"))).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture<>(new MockResponse(200, "ok", NOT_FOUND_RESPONSE)));

        // when
        MultiGetResponse response = requestBuilder.setRequestDocuments(asList(multiGetRequestDocumentBuilder().withId("V1").build())).execute();

        //then
        verify(boundRequestBuilderMock).setBody("{\"docs\":[{\"_id\":\"V1\"}]}");
        verify(httpClient).preparePost("/some-index/_mget");
        assertThat(response.getMultiGetResponseDocuments(), hasSize(1));
        assertThat(response.getMultiGetResponseDocuments().get(0), is(new MultiGetResponseDocument("V1", false, new JsonObject())));
    }

}