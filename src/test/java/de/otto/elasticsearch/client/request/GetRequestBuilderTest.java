package de.otto.elasticsearch.client.request;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.ning.http.client.AsyncHttpClient;
import de.otto.elasticsearch.client.CompletedFuture;
import de.otto.elasticsearch.client.MockResponse;
import de.otto.elasticsearch.client.response.GetResponse;
import de.otto.elasticsearch.client.response.HttpServerErrorException;
import de.otto.elasticsearch.client.util.RoundRobinLoadBalancingHttpClient;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class GetRequestBuilderTest {

    @Mock
    RoundRobinLoadBalancingHttpClient httpClient;

    @Mock
    AsyncHttpClient.BoundRequestBuilder boundRequestBuilder;

    @BeforeMethod
    public void setUp() throws Exception {
        initMocks(this);
    }

    @Test
    public void shouldExecuteFlushIndex() throws Exception {
        // given
        when(httpClient.prepareGet("/someIndex/someType/someId")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "ok",
                "{\n" +
                        "  \"_index\": \"product_1461747600019\",\n" +
                        "  \"_type\": \"product\",\n" +
                        "  \"_id\": \"495096989\",\n" +
                        "  \"_version\": 1,\n" +
                        "  \"found\": true,\n" +
                        "  \"_source\": {\n" +
                        "    \"_id\": \"495096989\",\n" +
                        "    \"lastModified\": \"2016-04-27T11:06:00.323\",\n" +
                        "    \"onlineDate\": \"2015-06-22\"" +
                        "}\n" +
                        "}")));
        // when
        GetResponse response = new GetRequestBuilder(httpClient, "someIndex", "someType", "someId").execute();

        // then
        JsonObject jsonObject = new JsonObject();
        jsonObject.add("_id", new JsonPrimitive("495096989"));
        jsonObject.add("lastModified", new JsonPrimitive("2016-04-27T11:06:00.323"));
        jsonObject.add("onlineDate", new JsonPrimitive("2015-06-22"));

        assertThat(response.isExists(), is(true));
        assertThat(response.getSource(), is(jsonObject));
        verify(httpClient).prepareGet("/someIndex/someType/someId");
        verify(boundRequestBuilder).execute();
    }

    @Test(expectedExceptions = HttpServerErrorException.class)
    public void shouldThrowExceptionIfHttpStatusIsNotEqual400() throws Exception {
        // given
        when(httpClient.prepareGet("/someIndex/someType/someId")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(400, "not ok",
                "{}")));
        // when
        try {
            new GetRequestBuilder(httpClient, "someIndex", "someType", "someId").execute();
        } catch (HttpServerErrorException e) {
            assertThat(e.getMessage(), is("400 not ok"));
            assertThat(e.getResponseBody(), is("{}"));
            throw e;
        }
        // then
    }
}