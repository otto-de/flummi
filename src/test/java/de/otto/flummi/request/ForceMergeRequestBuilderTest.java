package de.otto.flummi.request;

import de.otto.flummi.CompletedFuture;
import de.otto.flummi.MockResponse;
import de.otto.flummi.util.HttpClientWrapper;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import org.asynchttpclient.BoundRequestBuilder;

public class ForceMergeRequestBuilderTest {

    private HttpClientWrapper httpClient;

    private BoundRequestBuilder boundRequestBuilder;

    @BeforeMethod
    public void setup() {
        httpClient = mock(HttpClientWrapper.class);
        boundRequestBuilder = mock(BoundRequestBuilder.class);
    }

    @Test
    public void shouldRefreshIndex() {
        // given
        ForceMergeRequestBuilder forceMergeRequestBuilder = new ForceMergeRequestBuilder(httpClient, "someIndexName");
        when(httpClient.preparePost(any(String.class))).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture<>(new MockResponse(200, "OK", "")));

        // when
        forceMergeRequestBuilder.execute();

        //then
        verify(httpClient).preparePost("/someIndexName/_forcemerge");
    }

}