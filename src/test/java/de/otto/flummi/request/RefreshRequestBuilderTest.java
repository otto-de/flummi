package de.otto.flummi.request;

import com.ning.http.client.AsyncHttpClient;
import de.otto.flummi.CompletedFuture;
import de.otto.flummi.MockResponse;
import de.otto.flummi.util.HttpClientWrapper;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class RefreshRequestBuilderTest {

    private HttpClientWrapper httpClient;

    private AsyncHttpClient.BoundRequestBuilder boundRequestBuilder;

    @BeforeMethod
    public void setup() {
        httpClient = mock(HttpClientWrapper.class);
        boundRequestBuilder = mock(AsyncHttpClient.BoundRequestBuilder.class);
    }

    @Test
    public void shouldRefreshIndex() {
        // given
        RefreshRequestBuilder refreshRequestBuilder = new RefreshRequestBuilder(httpClient, "someIndexName");
        when(httpClient.preparePost(any(String.class))).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture<>(new MockResponse(200, "OK", "")));

        // when
        refreshRequestBuilder.execute();

        //then
        verify(httpClient).preparePost("/someIndexName/_refresh");
    }

}