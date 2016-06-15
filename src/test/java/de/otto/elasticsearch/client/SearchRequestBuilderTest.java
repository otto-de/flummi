package de.otto.elasticsearch.client;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.ning.http.client.AsyncHttpClient;
import de.otto.elasticsearch.client.aggregations.NestedAggregationBuilder;
import de.otto.elasticsearch.client.aggregations.TermsBuilder;
import de.otto.elasticsearch.client.query.QueryBuilders;
import de.otto.elasticsearch.client.request.SearchRequestBuilder;
import de.otto.elasticsearch.client.response.SearchHit;
import de.otto.elasticsearch.client.response.SearchResponse;
import de.otto.elasticsearch.client.util.RoundRobinLoadBalancingHttpClient;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static de.otto.elasticsearch.client.SortOrder.ASC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class SearchRequestBuilderTest {

    public static final String EMPTY_SEARCH_RESPONSE = "{\"took\":1, \"hits\":{\"max_score\":null,\"total\":0, \"hits\":[]}}";
    public static final String PRODUCT_JSON = "{\"_id\":\"P0\",\"lastModified\":\"2016-04-14T09:17:46.888\",\"topProductScore\":0.0,\"promotable\":true,\"soldout\":false,\"sale\":false,\"largeSize\":false,\"premiumBrand\":false,\"sharedCategoryPath\":\"some\",\"brand\":\"adidas\",\"categories\":[{\"path\":\"some/path/p0/\",\"titlePath\":\"Some\\u003ePath\\u003eP0\",\"assortment\":\"spielzeug\",\"targetGroup\":\"herren\",\"uniqueId\":\"Some\\u003ePath\\u003eP0§spielzeug§herren\"},{\"path\":\"some/other/path/p0/\",\"titlePath\":\"Some\\u003eOther\\u003ePath\\u003eP0\",\"assortment\":\"sport\",\"targetGroup\":\"herren\",\"uniqueId\":\"Some\\u003eOther\\u003ePath\\u003eP0§sport§herren\"}],\"levelCategories\":{\"1\":{\"value\":[\"p0\",\"path\"],\"count\":2},\"2\":{\"value\":[\"p0\"],\"count\":1}},\"tags\":[],\"reductionClasses\":[],\"displayRestrictions\":[]}";
    public static final String SEARCH_RESPONSE_WITH_ONE_HIT = "{\"took\":1," +
            "\"timed_out\":false," +
            "\"_shards\":{\"total\":5,\"successful\":5,\"failed\":0}," +
            "\"hits\":{\"total\":1,\"max_score\":1.0,\"hits\":[" +
            "{\"_index\":\"product_1460618266743\",\"_type\":\"product\",\"_id\":\"P0\",\"_score\":1.0,\"_source\":" +
            PRODUCT_JSON +
            "}" +
            "]}}";
    SearchRequestBuilder searchRequestBuilder;
    RoundRobinLoadBalancingHttpClient httpClient;
    private ImmutableList<String> HOSTS = ImmutableList.of("someHost:9200");

    @BeforeMethod
    public void setUp() throws Exception {
        httpClient = mock(RoundRobinLoadBalancingHttpClient.class);
        searchRequestBuilder = new SearchRequestBuilder(httpClient, "some-index");
    }

    @Test
    public void shouldBuilderQueryWithOneField() throws Exception {
        // given
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(httpClient.preparePost("/some-index/_search")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBody(any(String.class))).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBodyEncoding(anyString())).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture<>(new MockResponse(200, "ok", EMPTY_SEARCH_RESPONSE)));

        // when
        SearchResponse response = searchRequestBuilder.setQuery(createSampleQuery()).addField("someField").execute();

        //then
        verify(boundRequestBuilderMock).setBody("{\"query\":{\"term\":{\"someField\":\"someValue\"}},\"fields\":[\"someField\"]}");
        verify(httpClient).preparePost("/some-index/_search");
    }

    @Test
    public void shouldParseSearchResponseWithFullDocuments() throws Exception {
        // given
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(httpClient.preparePost("/some-index/_search")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBody(any(String.class))).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBodyEncoding(anyString())).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture<>(new MockResponse(200, "ok", SEARCH_RESPONSE_WITH_ONE_HIT)));

        // when
        SearchResponse response = searchRequestBuilder.setQuery(createSampleQuery()).execute();

        //then
        verify(boundRequestBuilderMock).setBody("{\"query\":{\"term\":{\"someField\":\"someValue\"}}}");
        verify(httpClient).preparePost("/some-index/_search");

        assertThat(response.getTookInMillis(), is(1L));
        assertThat(response.getHits().getMaxScore(), is(1F));
        assertThat(response.getAggregations().size(), is(0));
        assertThat(response.getHits().getTotalHits(), is(1L));
        SearchHit firstHit = response.getHits().iterator().next();
        assertThat(firstHit.getFields().entrySet().size(), is(0));
        assertThat(firstHit.getId(), is("P0"));
        assertThat(firstHit.getScore(), is(1F));
        assertThat(firstHit.getSource(), is(new Gson().fromJson(PRODUCT_JSON, JsonObject.class)));
    }

    @Test
    public void shouldBuilderQueryWithFromAndSize() throws Exception {
        // given
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(httpClient.preparePost("/some-index/_search")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBody(any(String.class))).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBodyEncoding(anyString())).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture<>(new MockResponse(200, "ok", EMPTY_SEARCH_RESPONSE)));

        // when
        SearchResponse response = searchRequestBuilder.setQuery(createSampleQuery()).setSize(42).setFrom(111).execute();

        //then
        verify(boundRequestBuilderMock).setBody("{\"query\":{\"term\":{\"someField\":\"someValue\"}},\"from\":111,\"size\":42}");
        verify(httpClient).preparePost("/some-index/_search");
    }

    @Test
    public void shouldSetHttpRequestTimeout() throws Exception {
        // given
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(httpClient.preparePost("/some-index/_search")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBody(any(String.class))).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBodyEncoding(anyString())).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture<>(new MockResponse(200, "ok", EMPTY_SEARCH_RESPONSE)));

        // when
        SearchResponse response = searchRequestBuilder.setQuery(createSampleQuery()).setTimeoutMillis(54321).execute();

        //then
        verify(boundRequestBuilderMock).setBody("{\"query\":{\"term\":{\"someField\":\"someValue\"}}}");
        verify(boundRequestBuilderMock).setRequestTimeout(54321);
        verify(httpClient).preparePost("/some-index/_search");
    }

    @Test
    public void shouldBuildRequestWithOneSort() throws Exception {
        // given
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(httpClient.preparePost("/some-index/_search")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBody(any(String.class))).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBodyEncoding(anyString())).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture<>(new MockResponse(200, "ok", EMPTY_SEARCH_RESPONSE)));

        // when
        SearchResponse response = searchRequestBuilder.setQuery(createSampleQuery()).addSort("someKey", SortOrder.DESC).execute();

        //then
        verify(boundRequestBuilderMock).setBody("{\"query\":{\"term\":{\"someField\":\"someValue\"}},\"sort\":[{\"someKey\":{\"order\":\"desc\"}}]}");
        verify(httpClient).preparePost("/some-index/_search");
    }

    @Test
    public void shouldBuildRequestWithMultipleSort() throws Exception {
        // given
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(httpClient.preparePost("/some-index/_search")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBody(any(String.class))).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBodyEncoding(anyString())).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture<>(new MockResponse(200, "ok", EMPTY_SEARCH_RESPONSE)));

        // when
        SearchResponse response = searchRequestBuilder.setQuery(createSampleQuery()).addSort("someKey", SortOrder.DESC).addSort("someOtherKey", ASC).execute();

        //then
        verify(boundRequestBuilderMock).setBody("{\"query\":{\"term\":{\"someField\":\"someValue\"}},\"sort\":[{\"someKey\":{\"order\":\"desc\"}},{\"someOtherKey\":{\"order\":\"asc\"}}]}");
        verify(httpClient).preparePost("/some-index/_search");
    }

    @Test
    public void shouldBuildRequestWithPostFilter() throws Exception {
        // given
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(httpClient.preparePost("/some-index/_search")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBody(any(String.class))).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBodyEncoding(anyString())).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture<>(new MockResponse(200, "ok", EMPTY_SEARCH_RESPONSE)));

        // when
        SearchResponse response = searchRequestBuilder.setQuery(createSampleQuery()).setPostFilter(QueryBuilders.termQuery("someField", "someValue")).execute();

        //then
        verify(boundRequestBuilderMock).setBody("{\"query\":{\"term\":{\"someField\":\"someValue\"}},\"post_filter\":{\"term\":{\"someField\":\"someValue\"}}}");
        verify(httpClient).preparePost("/some-index/_search");
    }

    @Test
    public void shouldBuildRequestWithAggregations() throws Exception {
        // given
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(httpClient.preparePost("/some-index/_search")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBody(any(String.class))).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBodyEncoding(anyString())).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture<>(new MockResponse(200, "ok", EMPTY_SEARCH_RESPONSE)));

        // when
        SearchResponse response = searchRequestBuilder.setQuery(createSampleQuery()).addAggregation(new TermsBuilder("Katzenklo")
                .field("someField"))
                .execute();

        //then
        verify(boundRequestBuilderMock).setBody("{\"query\":{\"term\":{\"someField\":\"someValue\"}}," +
                "\"aggregations\":{\"Katzenklo\":{\"terms\":{\"field\":\"someField\"}}}}");
        verify(httpClient).preparePost("/some-index/_search");
    }

    @Test
    public void shouldBuildRequestWithNestedAggregations() throws Exception {
        // given
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);
        when(httpClient.preparePost("/some-index/_search")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBody(any(String.class))).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBodyEncoding(anyString())).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture<>(new MockResponse(200, "ok", EMPTY_SEARCH_RESPONSE)));

        // when
        SearchResponse response = searchRequestBuilder.setQuery(createSampleQuery())
                .addAggregation(new NestedAggregationBuilder("Katzenklo")
                        .path("somePath")
                        .subAggregation(new TermsBuilder("someAggregation").field("someField"))
                )
                .execute();

        //then
        verify(boundRequestBuilderMock).setBody(
                "{" +
                        "\"query\":{\"term\":{\"someField\":\"someValue\"}}," +
                        "\"aggregations\":{" +
                        "\"Katzenklo\":{" +
                        "\"nested\":{\"path\":\"somePath\"}," +
                        "\"aggregations\":{" +
                        "\"someAggregation\":{" +
                        "\"terms\":{\"field\":\"someField\"}" +
                        "}" +
                        "}" +
                        "}" +
                        "}" +
                        "}");
        verify(httpClient).preparePost("/some-index/_search");
    }


    private JsonObject createSampleQuery() {
        return QueryBuilders.termQuery("someField", "someValue").build();
    }
}