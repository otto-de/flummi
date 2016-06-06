package de.otto.elasticsearch.client;

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.ListenableFuture;
import com.ning.http.client.Response;
import de.otto.elasticsearch.client.bulkactions.DeleteActionBuilder;
import de.otto.elasticsearch.client.request.*;
import de.otto.elasticsearch.client.response.HttpServerErrorException;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class ElasticSearchHttpClientTest {

    private ElasticSearchHttpClient client;
    private AsyncHttpClient asyncHttpClient;
    private AsyncHttpClient.BoundRequestBuilder boundRequestBuilder;

    @BeforeMethod
    public void setUp() {
        boundRequestBuilder = mock(AsyncHttpClient.BoundRequestBuilder.class);
        asyncHttpClient = mock(AsyncHttpClient.class);
        client = new ElasticSearchHttpClient(asyncHttpClient, "someHost:9200");
        when(asyncHttpClient.prepareGet(anyString())).thenReturn(boundRequestBuilder);
        when(asyncHttpClient.prepareDelete(anyString())).thenReturn(boundRequestBuilder);
        when(asyncHttpClient.preparePost(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody(anyString())).thenReturn(boundRequestBuilder);
    }

    @Test
    public void shouldGetIndexNameForAlias() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{\"someIndexName\":{\"aliases\": {\"someAliasName\": {}}}}")));

        //When
        final Optional<String> indexNameForAlias = client.getIndexNameForAlias("someAliasName");

        //Then
        assertThat(indexNameForAlias.isPresent(), is(true));
        assertThat(indexNameForAlias.get(), is("someIndexName"));
    }

    @Test
    public void shouldNotGetIndexNameForAliasIfAliasNotExists() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{\"someIndexName\":{\"aliases\": {\"someOtherAliasName\": {}}}}")));

        //When
        final Optional<String> indexNameForAlias = client.getIndexNameForAlias("someAliasName");

        //Then
        assertThat(indexNameForAlias.isPresent(), is(false));
    }

    @Test(expectedExceptions = HttpServerErrorException.class)
    public void shouldNotGetIndexNameForAliasForErrorResponseCode() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(500, "Internal Server Error", "{\"someIndexName\":{\"aliases\": {\"someAliasName\": {}}}}")));

        //When
        client.getIndexNameForAlias("someAliasName");
    }

    @Test
    public void shouldDeleteIndex() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{\"acknowledged\":\"true\"}")));

        //When
        final boolean successful = client.deleteIndex(Lists.newArrayList("someIndexName"));

        //Then
        assertThat(successful, is(true));
        verify(asyncHttpClient).prepareDelete("http://someHost:9200/someIndexName");
    }

    @Test
    public void shouldNotDeleteIndexForAcknowledgedFalse() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{\"acknowledged\":\"false\"}")));

        //When
        final boolean successful = client.deleteIndex(Lists.newArrayList("someIndexName"));

        //Then
        assertThat(successful, is(false));
        verify(asyncHttpClient).prepareDelete("http://someHost:9200/someIndexName");
    }

    @Test(expectedExceptions = HttpServerErrorException.class)
    public void shouldNotDeleteIndexForErrorResponseCodeFalse() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(500, "Internal Server Error", "{\"acknowledged\":\"true\"}")));

        //When
        client.deleteIndex(Lists.newArrayList("someIndexName"));
    }

    @Test
    public void shouldReturnIndexExists() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{\"someIndexName\":{}}")));

        //When
        final boolean indexExists = client.indexExists("someIndexName");

        //Then
        assertThat(indexExists, is(true));
        verify(asyncHttpClient).prepareGet("http://someHost:9200/someIndexName");
    }

    @Test
    public void shouldReturnIndexNotExistsForErrorResponseCode() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(500, "Internal Server Error", "{\"someIndexName\":{}}")));

        //When
        final boolean indexExists = client.indexExists("someIndexName");

        //Then
        assertThat(indexExists, is(false));
        verify(asyncHttpClient).prepareGet("http://someHost:9200/someIndexName");
    }

    @Test
    public void shouldReturnIndexNotExistsForEmptyResponse() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{}")));

        //When
        final boolean indexExists = client.indexExists("someIndexName");

        //Then
        assertThat(indexExists, is(false));
        verify(asyncHttpClient).prepareGet("http://someHost:9200/someIndexName");
    }

    @Test
    public void shouldPointProductAliasToCurrentIndex() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{\"acknowledged\":true}")));

        //When
        client.pointProductAliasToCurrentIndex("someAliasName", "someIndexName");

        //Then
        verify(asyncHttpClient).preparePost("http://someHost:9200/_aliases");
        verify(boundRequestBuilder).setBody("{\"actions\":[{\"remove\":{\"index\":\"*\",\"alias\":\"someAliasName\"}},{\"add\":{\"index\":\"someIndexName\",\"alias\":\"someAliasName\"}}]}");
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void shouldNotPointProductAliasToCurrentIndexForAcknowledgedFalse() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{\"acknowledged\":false}")));

        //When
        client.pointProductAliasToCurrentIndex("someAliasName", "someIndexName");
    }

    @Test(expectedExceptions = HttpServerErrorException.class)
    public void shouldNotPointProductAliasToCurrentIndexForErrorResponseCode() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(500, "Internal Server Error", "{\"errors\":\"true\"}")));

        //When
        client.pointProductAliasToCurrentIndex("someAliasName", "someIndexName");
    }

    @Test
    public void shouldReturnAliasExists() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{\"someIndexName\":{\"aliases\": {\"someAlias\": {}}}}")));

        //When
        final boolean aliasExists = client.aliasExists("someAlias");

        //Then
        assertThat(aliasExists, is(true));
        verify(asyncHttpClient).prepareGet("http://someHost:9200/_aliases");
    }

    @Test
    public void shouldReturnAliasNotExists() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{\"someIndexName\":{\"aliases\": {\"someOtherAlias\": {}}}}")));

        //When
        final boolean aliasExists = client.aliasExists("someAlias");

        //Then
        assertThat(aliasExists, is(false));
        verify(asyncHttpClient).prepareGet("http://someHost:9200/_aliases");
    }

    @Test(expectedExceptions = HttpServerErrorException.class)
    public void shouldReturnAliasNotExistsFor500() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(500, "Internal Server Error", "{\"someIndexName\":{\"aliases\": {\"someAlias\": {}}}}")));

        //When
        client.aliasExists("someAlias");
    }

    @Test
    public void shouldGetAllIndexNames() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{\"someIndexName\":{}, \"someIndexName2\":{}, \"someIndexName3\":{}}")));

        //When
        final List<String> allIndexNames = client.getAllIndexNames();

        //Then
        verify(asyncHttpClient).prepareGet("http://someHost:9200/_all");
        assertThat(allIndexNames, hasSize(3));
        assertThat(allIndexNames.get(0), is("someIndexName"));
        assertThat(allIndexNames.get(1), is("someIndexName2"));
        assertThat(allIndexNames.get(2), is("someIndexName3"));
    }

    @Test
    public void shouldNotGetAllIndexNamesForEmptyResponse() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{}")));

        //When
        final List<String> allIndexNames = client.getAllIndexNames();

        //Then
        verify(asyncHttpClient).prepareGet("http://someHost:9200/_all");
        assertThat(allIndexNames, hasSize(0));
    }

    @Test(expectedExceptions = HttpServerErrorException.class)
    public void shouldNotGetAllIndexNamesForErrorResponseCode() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(500, "OK", "{}")));

        //When
        client.getAllIndexNames();
    }

    @Test
    public void shouldGetIndexSettings() throws ExecutionException, InterruptedException, IOException {
        //Given
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
        final JsonObject indexSettings = client.getIndexSettings();

        //Then
        final JsonObject indexJsonObject = indexSettings.get("jobs-test").getAsJsonObject().get("settings").getAsJsonObject().get("index").getAsJsonObject();
        assertThat(indexJsonObject.get("creation_date").getAsString(), is("1461567339233"));
        assertThat(indexJsonObject.get("number_of_shards").getAsString(), is("5"));
        assertThat(indexJsonObject.get("number_of_replicas").getAsString(), is("0"));
        assertThat(indexJsonObject.get("uuid").getAsString(), is("Ua_7izawQUaSYclxHyWxUA"));
        assertThat(indexJsonObject.get("version").getAsJsonObject().get("created").getAsString(), is("1070199"));
        verify(asyncHttpClient).prepareGet("http://someHost:9200/_all/_settings");
    }

    @Test(expectedExceptions = HttpServerErrorException.class)
    public void shouldGetIndexSettingsForErrorResponseCode() throws ExecutionException, InterruptedException, IOException {
        //Given
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
        client.getIndexSettings();
    }

    @Test
    public void shouldGetIndexSettingsForEmptyResponse() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{}")));

        //When
        final JsonObject indexSettings = client.getIndexSettings();

        //Then
        assertThat(indexSettings.entrySet().size(), is(0));
    }

    @Test
    public void shouldRefreshIndex() throws ExecutionException, InterruptedException {
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{}")));

        //When
        client.refreshIndex("someIndexName");

        //Then
        verify(asyncHttpClient).preparePost("http://someHost:9200/someIndexName/_refresh");
    }

    @Test(expectedExceptions = HttpServerErrorException.class)
    public void shouldFailToRefreshIndexForErrorResponse() throws ExecutionException, InterruptedException {
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(500, "Internal Server Error", "{\"errors\":\"true\"}")));

        //When
        client.refreshIndex("someIndexName");
    }

    @Test
    public void shouldPrepareSearch() throws ExecutionException, InterruptedException, IOException {
        //Given
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{\"took\":123, \"hits\": {\"total\": 3, \"max_score\":1, \"hits\": []}}")));
        when(boundRequestBuilder.setBodyEncoding(anyString())).thenReturn(boundRequestBuilder);
        //When
        final SearchRequestBuilder searchRequestBuilder = client.prepareSearch("someIndexName");
        searchRequestBuilder.execute();

        //Then
        verify(asyncHttpClient).preparePost("http://someHost:9200/someIndexName/_search");
        verify(boundRequestBuilder).setBodyEncoding("UTF-8");
        verify(boundRequestBuilder).setBody("{}");
    }

    @Test
    public void shouldPrepareCount() throws ExecutionException, InterruptedException, IOException {
        //Given
        final AsyncHttpClient.BoundRequestBuilder boundRequestBuilder = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(asyncHttpClient.prepareGet(anyString())).thenReturn(boundRequestBuilder);
        final ListenableFuture listenableFuture = mock(ListenableFuture.class);
        when(boundRequestBuilder.execute()).thenReturn(listenableFuture);
        when(boundRequestBuilder.setBody(anyString())).thenReturn(boundRequestBuilder);
        final Response response = mock(Response.class);
        when(listenableFuture.get()).thenReturn(response);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn("{\"count\":201}");
        final CountRequestBuilder countRequestBuilder = client.prepareCount("someIndexName");

        //When
        countRequestBuilder.execute();

        //Then
        verify(asyncHttpClient).prepareGet("http://someHost:9200/someIndexName/_count");
    }

    @Test
    public void shouldPrepareBulk() throws ExecutionException, InterruptedException, IOException {
        //Given
        final AsyncHttpClient.BoundRequestBuilder boundRequestBuilder = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(asyncHttpClient.preparePost(anyString())).thenReturn(boundRequestBuilder);
        final ListenableFuture listenableFuture = mock(ListenableFuture.class);
        when(boundRequestBuilder.execute()).thenReturn(listenableFuture);
        when(boundRequestBuilder.setBody(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBodyEncoding(anyString())).thenReturn(boundRequestBuilder);
        final Response response = mock(Response.class);
        when(listenableFuture.get()).thenReturn(response);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn("{\"errors\":false}");
        final BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        bulkRequestBuilder.add(new DeleteActionBuilder("someIndexName", "someId", "someType"));

        //When
        bulkRequestBuilder.execute();

        //Then
        verify(asyncHttpClient).preparePost("http://someHost:9200/_bulk");
    }

    @Test
    public void shouldPrepareGet() throws ExecutionException, InterruptedException, IOException {
        //Given
        final AsyncHttpClient.BoundRequestBuilder boundRequestBuilder = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(asyncHttpClient.prepareGet(anyString())).thenReturn(boundRequestBuilder);
        final ListenableFuture listenableFuture = mock(ListenableFuture.class);
        when(boundRequestBuilder.execute()).thenReturn(listenableFuture);
        final Response response = mock(Response.class);
        when(listenableFuture.get()).thenReturn(response);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn("{}");
        final GetRequestBuilder getRequestBuilder = client.prepareGet("someIndexName", "someDocumentType", "someProductId");

        //When
        getRequestBuilder.execute();

        //Then
        verify(asyncHttpClient).prepareGet("http://someHost:9200/someIndexName/someDocumentType/someProductId");
    }

    @Test
    public void shouldPrepareDeleteByName() throws ExecutionException, InterruptedException, IOException {
        //Given
        final AsyncHttpClient.BoundRequestBuilder boundRequestBuilder = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(asyncHttpClient.prepareDelete(anyString())).thenReturn(boundRequestBuilder);
        final ListenableFuture listenableFuture = mock(ListenableFuture.class);
        when(boundRequestBuilder.execute()).thenReturn(listenableFuture);
        final Response response = mock(Response.class);
        when(listenableFuture.get()).thenReturn(response);
        when(response.getStatusCode()).thenReturn(200);
        final DeleteIndexRequestBuilder deleteIndexRequestBuilder = client.prepareDeleteByName("someIndexName");

        //When
        deleteIndexRequestBuilder.execute();

        //Then
        verify(asyncHttpClient).prepareDelete("http://someHost:9200/someIndexName");
    }

    @Test
    public void shouldPrepareDelete() throws ExecutionException, InterruptedException, IOException {
        //Given
        final AsyncHttpClient.BoundRequestBuilder boundRequestBuilder = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(asyncHttpClient.prepareDelete(anyString())).thenReturn(boundRequestBuilder);
        final ListenableFuture listenableFuture = mock(ListenableFuture.class);
        when(boundRequestBuilder.execute()).thenReturn(listenableFuture);
        final Response response = mock(Response.class);
        when(listenableFuture.get()).thenReturn(response);
        when(response.getStatusCode()).thenReturn(200);
        final DeleteRequestBuilder deleteRequestBuilder = client.prepareDelete();
        deleteRequestBuilder.setIndexName("someIndexName");
        deleteRequestBuilder.setDocumentType("someDocumentType");
        deleteRequestBuilder.setId("someId");

        //When
        deleteRequestBuilder.execute();

        //Then
        verify(asyncHttpClient).prepareDelete("http://someHost:9200/someIndexName/someDocumentType/someId");
    }

    @Test
    public void shouldPrepareIndex() throws ExecutionException, InterruptedException, IOException {
        //Given
        final AsyncHttpClient.BoundRequestBuilder boundRequestBuilder = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(asyncHttpClient.preparePost(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBodyEncoding(anyString())).thenReturn(boundRequestBuilder);
        final ListenableFuture listenableFuture = mock(ListenableFuture.class);
        when(boundRequestBuilder.execute()).thenReturn(listenableFuture);
        final Response response = mock(Response.class);
        when(listenableFuture.get()).thenReturn(response);
        when(response.getStatusCode()).thenReturn(200);
        final IndexRequestBuilder indexRequestBuilder = client.prepareIndex();
        indexRequestBuilder.setSource(new JsonObject());

        //When
        indexRequestBuilder.execute();

        //Then
        verify(asyncHttpClient).preparePost("http://someHost:9200");
    }

    @Test
    public void shouldGetAdminClientCluster() throws ExecutionException, InterruptedException, IOException {
        // Given
        final AsyncHttpClient.BoundRequestBuilder boundRequestBuilder = mock(AsyncHttpClient.BoundRequestBuilder.class);
        final ListenableFuture<Response> listenableFuture = mock(ListenableFuture.class);
        final Response response = mock(Response.class);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn("{\"status\":\"GREEN\", \"cluster_name\":\"someClusterName\", \"timed_out\":\"someTimedOut\"}");
        when(listenableFuture.get()).thenReturn(response);
        when(boundRequestBuilder.execute()).thenReturn(listenableFuture);
        when(asyncHttpClient.prepareGet(anyString())).thenReturn(boundRequestBuilder);
        final ClusterAdminClient cluster = client.admin().cluster();
        final ClusterHealthRequestBuilder clusterHealthRequestBuilder = cluster.prepareHealth("someIndexName");

        //When
        clusterHealthRequestBuilder.execute();

        //Then
        Mockito.verify(asyncHttpClient).prepareGet("http://someHost:9200/_cluster/health/someIndexName");
        assertThat(cluster, notNullValue());
    }

    @Test
    public void shouldGetAdminClientIndices() throws ExecutionException, InterruptedException, IOException {
        //Given
        final AsyncHttpClient.BoundRequestBuilder boundRequestBuilder = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(asyncHttpClient.preparePut(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBodyEncoding(anyString())).thenReturn(boundRequestBuilder);
        final ListenableFuture<Response> listenableFuture = mock(ListenableFuture.class);
        when(boundRequestBuilder.execute()).thenReturn(listenableFuture);
        final Response response = mock(Response.class);
        when(listenableFuture.get()).thenReturn(response);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getResponseBody()).thenReturn("{\"acknowledged\":\"true\"}");
        final IndicesAdminClient indicesAdminClient = client.admin().indices();
        final CreateIndexRequestBuilder createIndexRequestBuilder = indicesAdminClient.prepareCreate("someIndexName");

        //When
        createIndexRequestBuilder.execute();

        //Then
        verify(asyncHttpClient).preparePut("http://someHost:9200/someIndexName");
        assertThat(indicesAdminClient, notNullValue());
    }
}