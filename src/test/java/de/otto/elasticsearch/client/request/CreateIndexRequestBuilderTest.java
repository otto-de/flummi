package de.otto.elasticsearch.client.request;

import com.google.common.collect.ImmutableList;
import com.ning.http.client.AsyncHttpClient;
import de.otto.elasticsearch.client.CompletedFuture;
import de.otto.elasticsearch.client.InvalidElasticsearchResponseException;
import de.otto.elasticsearch.client.MockResponse;
import de.otto.elasticsearch.client.response.HttpServerErrorException;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static de.otto.elasticsearch.client.request.GsonHelper.object;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class CreateIndexRequestBuilderTest {

    private ImmutableList<String> HOSTS = ImmutableList.of("someHost:9200");

    @Mock
    AsyncHttpClient asyncHttpClient;

    @Mock
    AsyncHttpClient.BoundRequestBuilder boundRequestBuilder;

    CreateIndexRequestBuilder testee;

    @BeforeMethod
    public void setUp() throws Exception {
        initMocks(this);
        testee = new CreateIndexRequestBuilder(asyncHttpClient, HOSTS, 0, "someIndex");
    }

    @Test
    public void shouldExecuteCreateIndexRequestWithMappings() throws Exception {
        // given
        when(asyncHttpClient.preparePut("http://" + HOSTS.get(0) + "/someIndex")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody(any(String.class))).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBodyEncoding(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "ok", "{\"acknowledged\": true}")));
        testee
                .setMappings(object("someType", object("someField", object("someSetting", "someValue"))));
        // when
        testee.execute();

        // then
        verify(asyncHttpClient).preparePut("http://" + HOSTS.get(0) + "/someIndex");
        verify(boundRequestBuilder).execute();
        verify(boundRequestBuilder).setBody("{\"mappings\":{\"someType\":{\"someField\":{\"someSetting\":\"someValue\"}}}}");
    }

    @Test
    public void shouldExecuteCreateIndexRequestWithSettings() throws Exception {
        // given
        when(asyncHttpClient.preparePut("http://" + HOSTS.get(0) + "/someIndex")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody(any(String.class))).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBodyEncoding(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "ok", "{\"acknowledged\": true}")));
        testee
                .setSettings(object("someSetting", "someValue"));
        // when
        testee.execute();

        // then
        verify(asyncHttpClient).preparePut("http://" + HOSTS.get(0) + "/someIndex");
        verify(boundRequestBuilder).execute();
        verify(boundRequestBuilder).setBody("{\"settings\":{\"someSetting\":\"someValue\"}}");
    }

    @Test(expectedExceptions = HttpServerErrorException.class)
    public void shouldThrowWhenServerReturnsBadStatusCode() throws Exception {
        // given
        when(asyncHttpClient.preparePut("http://" + HOSTS.get(0) + "/someIndex")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody(any(String.class))).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBodyEncoding(anyString())).thenReturn(boundRequestBuilder);
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
            assertThat(e.getMessage(), is("400 Bad Request"));
            assertThat(e.getResponseBody(), is("someBadBody"));
            throw e;
        }
    }

    @Test(expectedExceptions = InvalidElasticsearchResponseException.class)
    public void shouldThrowWhenServerReturnsAcknowledgedFalse() throws Exception {
        // given
        when(asyncHttpClient.preparePut("http://" + HOSTS.get(0) + "/someIndex")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody(any(String.class))).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBodyEncoding(anyString())).thenReturn(boundRequestBuilder);
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
