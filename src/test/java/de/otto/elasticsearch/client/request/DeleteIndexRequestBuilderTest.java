package de.otto.elasticsearch.client.request;

import com.google.common.collect.ImmutableList;
import com.ning.http.client.AsyncHttpClient;
import de.otto.elasticsearch.client.CompletedFuture;
import de.otto.elasticsearch.client.MockResponse;
import de.otto.elasticsearch.client.response.HttpServerErrorException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.*;

public class DeleteIndexRequestBuilderTest {

    private AsyncHttpClient asyncHttpClient;
    private DeleteIndexRequestBuilder testee;
    private ImmutableList<String> HOSTS = ImmutableList.of("someHost:9200");

    @BeforeMethod
    private void setup() {
        asyncHttpClient = mock(AsyncHttpClient.class);
        testee = new DeleteIndexRequestBuilder(asyncHttpClient, HOSTS, 0, "someIndexName");
    }

    @Test
    public void shouldDeleteIndex() {
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);

        when(asyncHttpClient.prepareDelete("http://" + HOSTS.get(0) + "/someIndexName")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "ok", "")));
        testee.execute();
        verify(asyncHttpClient).prepareDelete("http://someHost:9200/someIndexName");
    }

    @Test(expectedExceptions = HttpServerErrorException.class)
    public void shouldThrowExceptionIfStatusCodeNotOk() {
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(asyncHttpClient.prepareDelete("http://" + HOSTS.get(0) + "/someIndexName")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture(new MockResponse(400, "not ok", "")));
        try {
            testee.execute();
        } catch (HttpServerErrorException e) {
            assertThat(e.getMessage(), is("400 not ok"));
            throw e;
        }
    }

}