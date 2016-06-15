package de.otto.elasticsearch.client.request;

import com.google.common.collect.ImmutableList;
import com.ning.http.client.AsyncHttpClient;
import de.otto.elasticsearch.client.*;
import de.otto.elasticsearch.client.response.HttpServerErrorException;
import de.otto.elasticsearch.client.util.RoundRobinLoadBalancingHttpClient;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;

public class ClusterHealthRequestBuilderTest {

    private RoundRobinLoadBalancingHttpClient asyncHttpClient;
    private ClusterHealthRequestBuilder testee;

    @BeforeMethod
    public void setUp() throws Exception {
        asyncHttpClient = mock(RoundRobinLoadBalancingHttpClient.class);
        testee = new ClusterHealthRequestBuilder(asyncHttpClient, new String[]{"someIndex", "someOtherIndex"});
    }

    @Test
    public void shouldExecuteSimpleRequest() throws Exception {
        // given
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);

        Mockito.when(asyncHttpClient.prepareGet("/_cluster/health/someIndex,someOtherIndex")).thenReturn(boundRequestBuilderMock);
        Mockito.when(boundRequestBuilderMock.addQueryParam(anyString(), anyString())).thenReturn(boundRequestBuilderMock);
        Mockito.when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "ok",
                "{\n" +
                        "  \"cluster_name\": \"myCluster\",\n" +
                        "  \"status\": \"green\",\n" +
                        "  \"timed_out\": false" +
                        "}")));

        // when
        ClusterHealthResponse healthResponse = testee.execute();

        // then
        assertThat(healthResponse.getStatus(), is(ClusterHealthStatus.GREEN));
        assertThat(healthResponse.getCluster_name(), is("myCluster"));
        assertThat(healthResponse.isTimedOut(), is(false));
    }

    @Test
    public void shouldExecuteRequestWithTimeout() throws Exception {
        // given
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);

        Mockito.when(asyncHttpClient.prepareGet("/_cluster/health/someIndex,someOtherIndex")).thenReturn(boundRequestBuilderMock);
        Mockito.when(boundRequestBuilderMock.addQueryParam(anyString(), anyString())).thenReturn(boundRequestBuilderMock);
        Mockito.when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "ok",
                "{\n" +
                        "  \"cluster_name\": \"myCluster\",\n" +
                        "  \"status\": \"green\",\n" +
                        "  \"timed_out\": false" +
                        "}")));

        // when
        ClusterHealthResponse healthResponse = testee.setTimeout(1000).execute();

        // then
        assertThat(healthResponse.getStatus(), is(ClusterHealthStatus.GREEN));
        assertThat(healthResponse.getCluster_name(), is("myCluster"));
        assertThat(healthResponse.isTimedOut(), is(false));
        Mockito.verify(boundRequestBuilderMock).addQueryParam("timeout", "1000ms");
    }

    @Test
    public void shouldExecuteRequestWithWaitForYellowStatus() throws Exception {
        // given
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);

        Mockito.when(asyncHttpClient.prepareGet("/_cluster/health/someIndex,someOtherIndex")).thenReturn(boundRequestBuilderMock);
        Mockito.when(boundRequestBuilderMock.addQueryParam(anyString(), anyString())).thenReturn(boundRequestBuilderMock);
        Mockito.when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "ok",
                "{\n" +
                        "  \"cluster_name\": \"myCluster\",\n" +
                        "  \"status\": \"green\",\n" +
                        "  \"timed_out\": false" +
                        "}")));

        // when
        ClusterHealthResponse healthResponse = testee.setWaitForYellowStatus().execute();

        // then
        assertThat(healthResponse.getStatus(), is(ClusterHealthStatus.GREEN));
        assertThat(healthResponse.getCluster_name(), is("myCluster"));
        assertThat(healthResponse.isTimedOut(), is(false));
        Mockito.verify(boundRequestBuilderMock).addQueryParam("wait_for_status", "yellow");
    }

    @Test
    public void shouldThrowExceptionIfStatusIsMissingInResponse() throws Exception {
        // given
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);

        Mockito.when(asyncHttpClient.prepareGet("/_cluster/health/someIndex,someOtherIndex")).thenReturn(boundRequestBuilderMock);
        Mockito.when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "ok",
                "{\n" +
                        "  \"cluster_name\": \"myCluster\",\n" +
                        "  \"timed_out\": false" +
                        "}")));

        // when
        try {
            testee.execute();
        } catch (InvalidElasticsearchResponseException e) {
            assertThat(e.getMessage(), is("Missing response field: status"));
        }

        // then
    }

    @Test
    public void shouldThrowExceptionIfTimedOutIsMissingInResponse() throws Exception {
        // given
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);

        Mockito.when(asyncHttpClient.prepareGet("/_cluster/health/someIndex,someOtherIndex")).thenReturn(boundRequestBuilderMock);
        Mockito.when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "ok",
                "{\n" +
                        "  \"cluster_name\": \"myCluster\",\n" +
                        "  \"status\": \"red\"" +
                        "}")));

        // when
        try {
            testee.execute();
        } catch (InvalidElasticsearchResponseException e) {
            assertThat(e.getMessage(), is("Missing response field: timed_out"));
        }

        // then
    }

    @Test
    public void shouldThrowExceptionIfClusterNameIsMissingInResponse() throws Exception {
        // given
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);

        Mockito.when(asyncHttpClient.prepareGet("/_cluster/health/someIndex,someOtherIndex")).thenReturn(boundRequestBuilderMock);
        Mockito.when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "ok",
                "{\n" +
                        "  \"timed_out\": false,\n" +
                        "  \"status\": \"red\"" +
                        "}")));

        // when
        try {
            testee.execute();
        } catch (InvalidElasticsearchResponseException e) {
            assertThat(e.getMessage(), is("Missing response field: cluster_name"));
        }

        // then
    }

    @Test
    public void shouldThrowExceptionIfTimedOutIsTrue() throws Exception {
        // given
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);

        Mockito.when(asyncHttpClient.prepareGet("/_cluster/health/someIndex,someOtherIndex")).thenReturn(boundRequestBuilderMock);
        Mockito.when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "ok",
                "{\n" +
                        "  \"cluster_name\": \"myCluster\",\n" +
                        "  \"timed_out\": true,\n" +
                        "  \"status\": \"red\"" +
                        "}")));

        // when
        try {
            testee.execute();
        } catch (InvalidElasticsearchResponseException e) {
            assertThat(e.getMessage(), is("Timed out waiting for yellow cluster status"));
        }

        // then
    }

    @Test(expectedExceptions = HttpServerErrorException.class)
    public void shouldThrowExceptionIfStatusCodeNotOk() throws Exception {
        // given
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);

        Mockito.when(asyncHttpClient.prepareGet("/_cluster/health/someIndex,someOtherIndex")).thenReturn(boundRequestBuilderMock);
        Mockito.when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture(new MockResponse(400, "not ok",
                "{\n" +
                        "  \"cluster_name\": \"myCluster\",\n" +
                        "  \"timed_out\": false,\n" +
                        "  \"status\": \"red\"" +
                        "}")));

        // when
        try {
            testee.execute();
        }
        // then
        catch (HttpServerErrorException e) {
            assertThat(e.getMessage(), is("400 not ok"));
            assertThat(e.getResponseBody(), is("{\n  \"cluster_name\": \"myCluster\",\n  \"timed_out\": false,\n  \"status\": \"red\"}"));
            throw e;
        }
    }

}