package de.otto.flummi;

import com.google.gson.JsonObject;
import de.otto.flummi.request.CreateIndexRequestBuilder;
import de.otto.flummi.request.DeleteIndexRequestBuilder;
import de.otto.flummi.request.IndicesExistsRequestBuilder;
import de.otto.flummi.response.HttpServerErrorException;
import de.otto.flummi.util.HttpClientWrapper;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Response;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class IndicesAdminClientTest {
    private IndicesAdminClient indicesAdminClient;
    private HttpClientWrapper httpClient;
    private BoundRequestBuilder boundRequestBuilder;

    @BeforeMethod
    public void setup() {
        httpClient = mock(HttpClientWrapper.class);
        indicesAdminClient = new IndicesAdminClient(httpClient);
        boundRequestBuilder = mock(BoundRequestBuilder.class);
        when(httpClient.prepareGet(anyString())).thenReturn(boundRequestBuilder);
/*        when(asyncHttpClient.prepareDelete(anyString())).thenReturn(boundRequestBuilder);
        when(asyncHttpClient.preparePost(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody(anyString())).thenReturn(boundRequestBuilder);
        */
    }

    @Test
    public void shouldPrepareCreate() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(httpClient.preparePut(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setCharset(Charset.forName("UTF-8"))).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.addHeader(anyString(),anyString())).thenReturn(boundRequestBuilder);
        final ListenableFuture<Response> listenableFuture = mock(ListenableFuture.class);
        when(boundRequestBuilder.execute()).thenReturn(listenableFuture);
        final Response response = mock(Response.class);
        when(listenableFuture.get()).thenReturn(response);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn("{\"acknowledged\":\"true\"}");
        final CreateIndexRequestBuilder createIndexRequestBuilder = indicesAdminClient.prepareCreate("someIndexName");

        //When
        createIndexRequestBuilder.execute();

        //Then
        verify(httpClient).preparePut("/someIndexName");
    }

    @Test
    public void shouldPrepareExists() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(httpClient.prepareHead(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.addHeader(anyString(),anyString())).thenReturn(boundRequestBuilder);
        final ListenableFuture<Response> listenableFuture = mock(ListenableFuture.class);
        when(boundRequestBuilder.execute()).thenReturn(listenableFuture);
        final Response response = mock(Response.class);
        when(listenableFuture.get()).thenReturn(response);
        when(response.getStatusCode()).thenReturn(200);
        final IndicesExistsRequestBuilder indicesExistsRequestBuilder = indicesAdminClient.prepareExists("someIndexName");

        //When
        indicesExistsRequestBuilder.execute();

        //Then
        verify(httpClient).prepareHead("/someIndexName");
    }

    @Test
    public void shouldPrepareDelete() throws ExecutionException, InterruptedException {
        //Given
        when(httpClient.prepareDelete(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.addHeader(anyString(),anyString())).thenReturn(boundRequestBuilder);
        final ListenableFuture<Response> listenableFuture = mock(ListenableFuture.class);
        when(boundRequestBuilder.execute()).thenReturn(listenableFuture);
        final Response response = mock(Response.class);
        when(listenableFuture.get()).thenReturn(response);
        when(response.getStatusCode()).thenReturn(200);
        final DeleteIndexRequestBuilder deleteIndexRequestBuilder = indicesAdminClient.prepareDelete("someIndexName");

        //When
        deleteIndexRequestBuilder.execute();

        //Then
        verify(httpClient).prepareDelete("/someIndexName");
    }

    @Test
    public void shouldReturnAliasExists() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(boundRequestBuilder.addHeader(anyString(),anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{\"someIndexName\":{\"aliases\": {\"someAlias\": {}}}}")));

        //When
        final boolean aliasExists = indicesAdminClient.aliasExists("someAlias");

        //Then
        assertThat(aliasExists, is(true));
        verify(httpClient).prepareGet("/_aliases");
    }

    @Test
    public void shouldReturnAliasNotExists() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(boundRequestBuilder.addHeader(anyString(),anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{\"someIndexName\":{\"aliases\": {\"someOtherAlias\": {}}}}")));

        //When
        final boolean aliasExists = indicesAdminClient.aliasExists("someAlias");

        //Then
        assertThat(aliasExists, is(false));
        verify(httpClient).prepareGet("/_aliases");
    }

    @Test(expectedExceptions = HttpServerErrorException.class)
    public void shouldReturnAliasNotExistsFor500() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(boundRequestBuilder.addHeader(anyString(),anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(500, "Internal Server Error", "{\"someIndexName\":{\"aliases\": {\"someAlias\": {}}}}")));

        //When
        indicesAdminClient.aliasExists("someAlias");
    }

    @Test
    public void shouldGetAllIndexNames() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(boundRequestBuilder.addHeader(anyString(),anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{\"someIndexName\":{}, \"someIndexName2\":{}, \"someIndexName3\":{}}")));

        //When
        final List<String> allIndexNames = indicesAdminClient.getAllIndexNames();

        //Then
        verify(httpClient).prepareGet("/_all");
        assertThat(allIndexNames, hasSize(3));
        assertThat(allIndexNames.get(0), is("someIndexName"));
        assertThat(allIndexNames.get(1), is("someIndexName2"));
        assertThat(allIndexNames.get(2), is("someIndexName3"));
    }

    @Test
    public void shouldNotGetAllIndexNamesForEmptyResponse() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(boundRequestBuilder.addHeader(anyString(),anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{}")));

        //When
        final List<String> allIndexNames = indicesAdminClient.getAllIndexNames();

        //Then
        verify(httpClient).prepareGet("/_all");
        assertThat(allIndexNames, hasSize(0));
    }

    @Test(expectedExceptions = HttpServerErrorException.class)
    public void shouldNotGetAllIndexNamesForErrorResponseCode() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(boundRequestBuilder.addHeader(anyString(),anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(500, "OK", "{}")));

        //When
        indicesAdminClient.getAllIndexNames();
    }

    @Test
    public void shouldGetIndexSettings() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(boundRequestBuilder.addHeader(anyString(),anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{\"jobs-test\": {\n" +
                "    \"settings\": {\n" +
                "      \"index\": {\n" +
                "        \"creation_date\": \"1461567339233\",\n" +
                "        \"number_of_shards\": \"5\",\n" +
                "        \"number_of_replicas\": \"0\",\n" +
                "        \"version\": {\n" +
                "          \"created\": \"1070199\"\n" +
                "        },\n" +
                "        \"uuid\": \"Ua_7izawQUaSYclxHyWxUA\"\n" +
                "      }\n" +
                "    }\n" +
                "  }}")));

        //When
        final JsonObject indexSettings = indicesAdminClient.getIndexSettings();

        //Then
        final JsonObject indexJsonObject = indexSettings.get("jobs-test").getAsJsonObject().get("settings").getAsJsonObject().get("index").getAsJsonObject();
        assertThat(indexJsonObject.get("creation_date").getAsString(), is("1461567339233"));
        assertThat(indexJsonObject.get("number_of_shards").getAsString(), is("5"));
        assertThat(indexJsonObject.get("number_of_replicas").getAsString(), is("0"));
        assertThat(indexJsonObject.get("uuid").getAsString(), is("Ua_7izawQUaSYclxHyWxUA"));
        assertThat(indexJsonObject.get("version").getAsJsonObject().get("created").getAsString(), is("1070199"));
        verify(httpClient).prepareGet("/_all/_settings");
    }

    @Test(expectedExceptions = HttpServerErrorException.class)
    public void shouldGetIndexSettingsForErrorResponseCode() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(boundRequestBuilder.addHeader(anyString(),anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(500, "Internal Server Error", "{\"jobs-test\": {\n" +
                "    \"settings\": {\n" +
                "      \"index\": {\n" +
                "        \"creation_date\": \"1461567339233\",\n" +
                "        \"number_of_shards\": \"5\",\n" +
                "        \"number_of_replicas\": \"0\",\n" +
                "        \"version\": {\n" +
                "          \"created\": \"1070199\"\n" +
                "        },\n" +
                "        \"uuid\": \"Ua_7izawQUaSYclxHyWxUA\"\n" +
                "      }\n" +
                "    }\n" +
                "  }}")));

        //When
        indicesAdminClient.getIndexSettings();
    }

    @Test
    public void shouldGetIndexSettingsForEmptyResponse() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(boundRequestBuilder.addHeader(anyString(),anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{}")));

        //When
        final JsonObject indexSettings = indicesAdminClient.getIndexSettings();

        //Then
        assertThat(indexSettings.entrySet().size(), is(0));
    }

    @Test
    public void shouldGetIndexMapping() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(boundRequestBuilder.addHeader(anyString(),anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", ""+
            "{\"mapping-test\" : {\n" +
            "   \"mappings\" : {\n" +
            "     \"default\" : {\n" +
            "       \"properties\" : {\n" +
            "         \"test-string\" : {\n" +
            "           \"type\" : \"string\"\n" +
            "         }\n" +    
            "       }\n" +
            "     }\n" +
            "   }\n" +
            " }}")));

        //When
        final JsonObject indexSettings = indicesAdminClient.getIndexMapping("mapping-test");

        //Then
        final JsonObject indexJsonObject = indexSettings
        		                                   .get("mapping-test").getAsJsonObject()
        		                                   .get("mappings").getAsJsonObject()
        		                                   .get("default").getAsJsonObject()
        		                                   .get("properties").getAsJsonObject();
        assertThat(indexJsonObject.get("test-string").getAsJsonObject().get("type").getAsString(), is("string"));
        verify(httpClient).prepareGet("/mapping-test/_mapping");
    }
    
    @Test
    public void shouldRefreshIndex() throws ExecutionException, InterruptedException {
        when(httpClient.preparePost(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.addHeader(anyString(),anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{}")));

        //When
        indicesAdminClient.prepareRefresh("someIndexName").execute();

        //Then
        verify(httpClient).preparePost("/someIndexName/_refresh");
    }

    @Test
    public void shouldForceMergeOfIndex() throws ExecutionException, InterruptedException {
        when(httpClient.preparePost(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.addHeader(anyString(),anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{}")));

        //When
        indicesAdminClient.forceMerge("someIndexName").execute();

        //Then
        verify(httpClient).preparePost("/someIndexName/_forcemerge");
    }

    @Test(expectedExceptions = HttpServerErrorException.class)
    public void shouldFailToRefreshIndexForErrorResponse() throws ExecutionException, InterruptedException {
        when(httpClient.preparePost(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.addHeader(anyString(),anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(500, "Internal Server Error", "{\"errors\":\"true\"}")));

        //When
        indicesAdminClient.prepareRefresh("someIndexName").execute();
    }

    @Test
    public void shouldPointProductAliasToCurrentIndex() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(httpClient.preparePost(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.addHeader(anyString(),anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{\"acknowledged\":true}")));

        //When
        indicesAdminClient.pointAliasToCurrentIndex("someAliasName", "someIndexName");

        //Then
        verify(httpClient).preparePost("/_aliases");
        verify(boundRequestBuilder).setBody("{\"actions\":[{\"remove\":{\"index\":\"*\",\"alias\":\"someAliasName\"}},{\"add\":{\"index\":\"someIndexName\",\"alias\":\"someAliasName\"}}]}");
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void shouldNotPointProductAliasToCurrentIndexForAcknowledgedFalse() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(boundRequestBuilder.addHeader(anyString(),anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{\"acknowledged\":false}")));

        //When
        indicesAdminClient.pointAliasToCurrentIndex("someAliasName", "someIndexName");
    }

    @Test(expectedExceptions = HttpServerErrorException.class)
    public void shouldNotPointProductAliasToCurrentIndexForErrorResponseCode() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(httpClient.preparePost(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.addHeader(anyString(),anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(500, "Internal Server Error", "{\"errors\":\"true\"}")));

        //When
        indicesAdminClient.pointAliasToCurrentIndex("someAliasName", "someIndexName");
    }




}