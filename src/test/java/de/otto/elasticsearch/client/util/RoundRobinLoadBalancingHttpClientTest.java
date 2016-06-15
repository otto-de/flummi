package de.otto.elasticsearch.client.util;

import com.google.common.collect.ImmutableList;
import com.ning.http.client.AsyncHttpClient;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.google.common.collect.ImmutableList.of;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

@Test
public class RoundRobinLoadBalancingHttpClientTest {
    private final ImmutableList<String> hosts = of("http://someHost:9200", "http://someOtherHost:9200");
    private AsyncHttpClient asyncHttpClient;
    private AsyncHttpClient.BoundRequestBuilder boundRequestBuilder;

    RoundRobinLoadBalancingHttpClient testee;

    @BeforeMethod
    public void setUp() throws Exception {
        asyncHttpClient = mock(AsyncHttpClient.class);
        boundRequestBuilder = mock(AsyncHttpClient.BoundRequestBuilder.class);
        testee = new RoundRobinLoadBalancingHttpClient(asyncHttpClient, hosts);
        when(asyncHttpClient.prepareGet(anyString())).thenReturn(boundRequestBuilder);
    }

    @Test
    public void shouldIncrementHostCounter() throws Exception {
        testee.prepareGet("/some/url").execute();

        verify(asyncHttpClient).prepareGet("http://someHost:9200/some/url");
        assertThat(testee.roundRobinCounter, is(1));
        verifyNoMoreInteractions(asyncHttpClient);
    }

    @Test
    public void shouldUseHostCounter() throws Exception {
        testee.roundRobinCounter = 1;
        testee.prepareGet("/some/url").execute();

        verify(asyncHttpClient).prepareGet("http://someOtherHost:9200/some/url");
        assertThat(testee.roundRobinCounter, is(2));
        verifyNoMoreInteractions(asyncHttpClient);
    }

    @Test
    public void souldDealWithIntOverflow() throws Exception {
        testee.roundRobinCounter = Integer.MIN_VALUE;
        testee.prepareGet("/some/url").execute();

        verify(asyncHttpClient).prepareGet("http://someHost:9200/some/url");
        assertThat(testee.roundRobinCounter, is(Integer.MIN_VALUE + 1));
        verifyNoMoreInteractions(asyncHttpClient);
    }

}