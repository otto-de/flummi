package de.otto.flummi.request;

import de.otto.flummi.CompletedFuture;
import de.otto.flummi.InvalidElasticsearchResponseException;
import de.otto.flummi.MockResponse;
import de.otto.flummi.response.HttpServerErrorException;
import de.otto.flummi.util.HttpClientWrapper;
import org.asynchttpclient.BoundRequestBuilder;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.Charset;

import static de.otto.flummi.request.GsonHelper.object;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class CreateIndexRequestBuilderTest {

    @Mock
    HttpClientWrapper httpClient;

    @Mock
    BoundRequestBuilder boundRequestBuilder;

    CreateIndexRequestBuilder testee;

    @BeforeMethod
    public void setUp() throws Exception {
        initMocks(this);
        testee = new CreateIndexRequestBuilder(httpClient, "someIndex");
    }

    @Test
    public void shouldExecuteCreateIndexRequestWithMappings() throws Exception {
        // given
        when(httpClient.preparePut("/someIndex")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody(any(String.class))).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setCharset(Charset.forName("UTF-8"))).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "ok", "{\"acknowledged\": true}")));
        testee
                .setMappings(object("someType", object("someField", object("someSetting", "someValue"))));
        // when
        testee.execute();

        // then
        verify(httpClient).preparePut("/someIndex");
        verify(boundRequestBuilder).execute();
        verify(boundRequestBuilder).setBody("{\"mappings\":{\"someType\":{\"someField\":{\"someSetting\":\"someValue\"}}}}");
    }

    @Test
    public void shouldExecuteCreateIndexRequestWithSettings() throws Exception {
        // given
        when(httpClient.preparePut("/someIndex")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody(any(String.class))).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setCharset(Charset.forName("UTF-8"))).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "ok", "{\"acknowledged\": true}")));
        testee
                .setSettings(object("someSetting", "someValue"));
        // when
        testee.execute();

        // then
        verify(httpClient).preparePut("/someIndex");
        verify(boundRequestBuilder).execute();
        verify(boundRequestBuilder).setBody("{\"settings\":{\"someSetting\":\"someValue\"}}");
    }

    @Test(expectedExceptions = HttpServerErrorException.class)
    public void shouldThrowWhenServerReturnsBadStatusCode() throws Exception {
        // given
        when(httpClient.preparePut("/someIndex")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody(any(String.class))).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setCharset(Charset.forName("UTF-8"))).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(400, "Bad Request", "someBadBody")));
        testee
                .setSettings(object("someSetting", "someValue"));
        // when
        try {
            testee.execute();
        }

        // then
        catch (HttpServerErrorException e) {
            assertThat(e.getStatusCode(), is(400));
            assertThat(e.getResponseBody(), is("someBadBody"));
            throw e;
        }
    }

    @Test(expectedExceptions = InvalidElasticsearchResponseException.class)
    public void shouldThrowWhenServerReturnsAcknowledgedFalse() throws Exception {
        // given
        when(httpClient.preparePut("/someIndex")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody(any(String.class))).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setCharset(Charset.forName("UTF-8"))).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", "{\"acknowledged\": false}")));
        testee
                .setSettings(object("someSetting", "someValue"));
        // when
        try {
            testee.execute();
        }

        // then
        catch (InvalidElasticsearchResponseException e) {
            assertThat(e.getMessage(), is("Invalid reply from Elastic Search: {\"acknowledged\": false}"));
            throw e;
        }
    }
}
