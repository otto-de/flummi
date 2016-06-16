package de.otto.elasticsearch.client.response;

import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import de.otto.elasticsearch.client.CompletedFuture;
import de.otto.elasticsearch.client.MockResponse;
import de.otto.elasticsearch.client.util.RoundRobinLoadBalancingHttpClient;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;
import java.util.stream.Stream;

import static de.otto.elasticsearch.client.request.GsonHelper.object;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.assertTrue;


public class ScrollingSearchHitsTest {

    public static final String NEXT_PAGE = "{\"took\":1," +
            "\"_scroll_id\":\"some_scroll_id\"," +
            "\"timed_out\":false," +
            "\"_shards\":{\"total\":5,\"successful\":5,\"failed\":0}," +
            "\"hits\":{\"total\":10,\"max_score\":1.0,\"hits\":[" +
            "{\"_index\":\"product_1460618266743\",\"_type\":\"product\",\"_id\":\"P2\",\"_score\":1.0,\"_source\":{}" +
            "}," +
            "{\"_index\":\"product_1460618266743\",\"_type\":\"product\",\"_id\":\"P3\",\"_score\":1.0,\"_source\":{}" +
            "}" +
            "]}}";

    public static final String EMPTY_PAGE = "{\"took\":1," +
            "\"_scroll_id\":\"some_scroll_id\"," +
            "\"timed_out\":false," +
            "\"_shards\":{\"total\":5,\"successful\":5,\"failed\":0}," +
            "\"hits\":{\"total\":10,\"max_score\":1.0,\"hits\":[]}}";

    private AsyncHttpClient.BoundRequestBuilder requestBuilder;
    private RoundRobinLoadBalancingHttpClient httpClient;

    @BeforeMethod
    public void setUp() throws Exception {
        requestBuilder = mock(AsyncHttpClient.BoundRequestBuilder.class);
        httpClient = mock(RoundRobinLoadBalancingHttpClient.class);
        when(requestBuilder.setBody(anyString())).thenReturn(requestBuilder);
    }

    @Test
    public void shouldIterateAPageWithoutInteractingWithTheHttpClient() throws Exception {
        ScrollingSearchHits testee = new ScrollingSearchHits(100, 1F, "someScrollId", "1m", someSearchHits("P0", "P1"), httpClient);

        Iterator<SearchHit> iterator = testee.iterator();
        assertThat(iterator.next().getId(), is("P0"));
        assertThat(iterator.next().getId(), is("P1"));
        verifyZeroInteractions(httpClient);
    }

    @Test
    public void shouldStopIteratingEmptyList() throws Exception {
        ScrollingSearchHits testee = new ScrollingSearchHits(100, 1F, "someScrollId", "1m", Collections.emptyList(), httpClient);

        Iterator<SearchHit> iterator = testee.iterator();
        assertThat(iterator.hasNext(), is(false));
        verifyZeroInteractions(httpClient);
    }

    @Test
    public void shouldFetchNextPage() throws Exception {
        when(requestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", NEXT_PAGE)));
        ScrollingSearchHits testee = new ScrollingSearchHits(100, 1F, "someScrollId", "1m", someSearchHits("P0", "P1"), httpClient);
        when(httpClient.preparePost(anyString())).thenReturn(requestBuilder);

        Iterator<SearchHit> iterator = testee.iterator();
        assertTrue(iterator.hasNext());
        assertThat(iterator.next().getId(), is("P0"));
        assertTrue(iterator.hasNext());
        assertThat(iterator.next().getId(), is("P1"));
        verifyZeroInteractions(httpClient);
        assertTrue(iterator.hasNext());
        assertThat(iterator.next().getId(), is("P2"));
        verify(httpClient).preparePost("/_search/scroll");
    }

    @Test
    public void shouldSpliterate() throws Exception {
        when(requestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", NEXT_PAGE)));
        ScrollingSearchHits testee = new ScrollingSearchHits(100, 1F, "someScrollId", "1m", someSearchHits("P0", "P1"), httpClient);
        when(httpClient.preparePost(anyString())).thenReturn(requestBuilder);

        Spliterator<SearchHit> spliterator = testee.spliterator();
        assertTrue(spliterator.tryAdvance(hit -> assertThat(hit.getId(), is("P0"))));
        assertTrue(spliterator.tryAdvance(hit -> assertThat(hit.getId(), is("P1"))));
        verifyZeroInteractions(httpClient);
        assertTrue(spliterator.tryAdvance(hit -> assertThat(hit.getId(), is("P2"))));
        verify(httpClient).preparePost("/_search/scroll");
    }

    @Test
    public void shouldFetchEmptyNextPage() throws Exception {
        when(requestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "OK", EMPTY_PAGE)));
        ScrollingSearchHits testee = new ScrollingSearchHits(100, 1F, "someScrollId", "1m", someSearchHits("P0", "P1"), httpClient);
        when(httpClient.preparePost(anyString())).thenReturn(requestBuilder);

        Iterator<SearchHit> iterator = testee.iterator();
        assertThat(iterator.next().getId(), is("P0"));
        assertThat(iterator.next().getId(), is("P1"));
        verifyZeroInteractions(httpClient);
        assertThat(iterator.hasNext(), is(false));
        verify(httpClient).preparePost("/_search/scroll");
        verify(requestBuilder).execute();
        verifyNoMoreInteractions(httpClient);
    }

    private List<SearchHit> someSearchHits(String... ids) {
        return Arrays.stream(ids).map(id -> new SearchHit(id, object(), null, 1F)).collect(toList());
    }
}