package de.otto.elasticsearch.client.request;

import com.google.common.collect.ImmutableList;
import com.ning.http.client.AsyncHttpClient;
import de.otto.elasticsearch.client.CompletedFuture;
import de.otto.elasticsearch.client.MockResponse;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class RefreshRequestBuilderTest {

    private AsyncHttpClient asyncHttpClient;

    private AsyncHttpClient.BoundRequestBuilder boundRequestBuilder;
    private ImmutableList<String> HOSTS = ImmutableList.of("someHost:9200");


    @BeforeMethod
    public void setup() {
        asyncHttpClient = mock(AsyncHttpClient.class);
        boundRequestBuilder = mock(AsyncHttpClient.BoundRequestBuilder.class);
    }

    @Test
    public void shouldRefreshIndex() {
        // given
        RefreshRequestBuilder refreshRequestBuilder = new RefreshRequestBuilder(asyncHttpClient, HOSTS, 0, "someIndexName");
        when(asyncHttpClient.preparePost(any(String.class))).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture<>(new MockResponse(200, "OK", "")));

        // when
        refreshRequestBuilder.execute();

        //then
        verify(asyncHttpClient).preparePost("http://someHost:9200/someIndexName/_refresh");
    }

}