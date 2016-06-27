package de.otto.elasticsearch.client.request;

import com.ning.http.client.AsyncHttpClient;
import de.otto.elasticsearch.client.CompletedFuture;
import de.otto.elasticsearch.client.InvalidElasticsearchResponseException;
import de.otto.elasticsearch.client.MockResponse;
import de.otto.elasticsearch.client.bulkactions.IndexActionBuilder;
import de.otto.elasticsearch.client.bulkactions.IndexOpType;
import de.otto.elasticsearch.client.response.HttpServerErrorException;
import de.otto.elasticsearch.client.util.HttpClientWrapper;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static de.otto.elasticsearch.client.request.GsonHelper.object;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class BulkRequestBuilderTest {

    BulkRequestBuilder testee;

    @Mock
    HttpClientWrapper asyncHttpClient;

    @BeforeMethod
    public void setUp() throws Exception {
        initMocks(this);
        testee = new BulkRequestBuilder(asyncHttpClient);
    }

    @Test
    public void shouldFireBulkRequest() throws Exception {
        // given
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);

        when(asyncHttpClient.preparePost("/_bulk")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBody("{\"index\":{\"_index\":\"someIndex\",\"_type\":\"Flutschfinger\"}}\n{\"Eis\":\"am Stiel\"}\n")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBodyEncoding("UTF-8")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "ok", "{\"errors\":false}")));

        testee.add(new IndexActionBuilder("someIndex").setOpType(IndexOpType.INDEX).setType("Flutschfinger").setSource(object("Eis", "am Stiel")));

        // when
        testee.execute();

        // then
        verify(asyncHttpClient).preparePost("/_bulk");
        verify(boundRequestBuilderMock).execute();
        verify(boundRequestBuilderMock).setBody("{\"index\":{\"_index\":\"someIndex\",\"_type\":\"Flutschfinger\"}}\n{\"Eis\":\"am Stiel\"}\n");
    }

    @Test(expectedExceptions = HttpServerErrorException.class)
    public void shouldThrowExceptionIfHttpStatusIsNot200() throws Exception {
        // given
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);

        when(asyncHttpClient.preparePost("/_bulk")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBody("{\"index\":{\"_index\":\"someIndex\",\"_type\":\"Flutschfinger\"}}\n{\"Eis\":\"am Stiel\"}\n")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBodyEncoding("UTF-8")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture(new MockResponse(400, "not ok", "{\"errors\":false}")));

        testee.add(new IndexActionBuilder("someIndex").setOpType(IndexOpType.INDEX).setType("Flutschfinger").setSource(object("Eis", "am Stiel")));

        // when
        try {
            testee.execute();
        }
        // then
        catch (HttpServerErrorException e) {
            assertThat(e.getMessage(), is("400 not ok"));
            assertThat(e.getResponseBody(), is("{\"errors\":false}"));
            throw e;
        }
    }

    @Test
    public void shouldThrowAnExceptionIfOneOpIsPresentAndItIsNotAnUpdate404() throws Exception {
        // given
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);

        when(asyncHttpClient.preparePost("/_bulk")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBody("{\"index\":{\"_index\":\"someIndex\",\"_type\":\"Flutschfinger\"}}\n{\"Eis\":\"am Stiel\"}\n")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBodyEncoding("UTF-8")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "ok", "{\"errors\":true,\"items\":[{\"index\":{\"status\":400,\"error\":\"someError\"}}]}")));

        testee.add(new IndexActionBuilder("someIndex").setOpType(IndexOpType.INDEX).setType("Flutschfinger").setSource(object("Eis", "am Stiel")));

        // when
        try {
            testee.execute();
        } catch (InvalidElasticsearchResponseException e) {
            assertThat(e.getMessage(), is("Response contains errors': {\"errors\":true,\"items\":[{\"index\":{\"status\":400,\"error\":\"someError\"}}]}"));
        }
        // then
    }

    @Test
    public void shouldNotThrowAnExceptionIfOneOpIsPresentAndItIsAnUpdate404() throws Exception {
        // given
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);

        when(asyncHttpClient.preparePost("/_bulk")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBody("{\"index\":{\"_index\":\"someIndex\",\"_type\":\"Flutschfinger\"}}\n{\"Eis\":\"am Stiel\"}\n")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBodyEncoding("UTF-8")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "ok", "{\"errors\":true,\"items\":[{\"update\":{\"status\":404,\"error\":\"someError\"}}]}")));

        testee.add(new IndexActionBuilder("someIndex").setOpType(IndexOpType.INDEX).setType("Flutschfinger").setSource(object("Eis", "am Stiel")));

        // when
        testee.execute();
        // then
    }

    @Test
    public void shouldThrowAnExceptionIfOneOpIsPresentAndItIsNotAnUpdate503() throws Exception {
        // given
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);

        when(asyncHttpClient.preparePost("/_bulk")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBody("{\"index\":{\"_index\":\"someIndex\",\"_type\":\"Flutschfinger\"}}\n{\"Eis\":\"am Stiel\"}\n")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBodyEncoding("UTF-8")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "ok", "{\"errors\":true,\"items\":[{\"index\":{\"status\":503,\"error\":\"someError\"}}]}")));

        testee.add(new IndexActionBuilder("someIndex").setOpType(IndexOpType.INDEX).setType("Flutschfinger").setSource(object("Eis", "am Stiel")));

        // when
        try {
            testee.execute();
        } catch (InvalidElasticsearchResponseException e) {
            assertThat(e.getMessage(), is("Response contains errors': {\"errors\":true,\"items\":[{\"index\":{\"status\":503,\"error\":\"someError\"}}]}"));
        }
        // then
    }

    @Test
    public void shouldThrowAnExceptionIfTwoOpsArePresentAndDeleteHasAnError() throws Exception {
        // given
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);

        when(asyncHttpClient.preparePost("/_bulk")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBody("{\"index\":{\"_index\":\"someIndex\",\"_type\":\"Flutschfinger\"}}\n{\"Eis\":\"am Stiel\"}\n")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBodyEncoding("UTF-8")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "ok", "{\"errors\":true,\"items\":[{\"create\":{\"status\":200}},{\"delete\":{\"status\":503,\"error\":\"someError\"}}]}")));

        testee.add(new IndexActionBuilder("someIndex").setOpType(IndexOpType.INDEX).setType("Flutschfinger").setSource(object("Eis", "am Stiel")));

        // when
        try {
            testee.execute();
        } catch (InvalidElasticsearchResponseException e) {
            assertThat(e.getMessage(), is("Response contains errors': {\"errors\":true,\"items\":[{\"create\":{\"status\":200}},{\"delete\":{\"status\":503,\"error\":\"someError\"}}]}"));
        }
        // then
    }

    @Test
    public void shouldNotThrowAnExceptionIfTwoOpsArePresentAndUpdateHasAn404Error() throws Exception {
        // given
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);

        when(asyncHttpClient.preparePost("/_bulk")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBody("{\"index\":{\"_index\":\"someIndex\",\"_type\":\"Flutschfinger\"}}\n{\"Eis\":\"am Stiel\"}\n")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBodyEncoding("UTF-8")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "ok", "{\"errors\":true,\"items\":[{\"create\":{\"status\":200}},{\"update\":{\"status\":404,\"error\":\"someError\"}}]}")));

        testee.add(new IndexActionBuilder("someIndex").setOpType(IndexOpType.INDEX).setType("Flutschfinger").setSource(object("Eis", "am Stiel")));

        // when
        testee.execute();
        // then
    }

    @Test
    public void shouldNotThrowExceptionForResponseItemWithoutErrors() throws Exception {
        // given
        AsyncHttpClient.BoundRequestBuilder boundRequestBuilderMock = mock(AsyncHttpClient.BoundRequestBuilder.class);

        when(asyncHttpClient.preparePost("/_bulk")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBody("{\"index\":{\"_index\":\"someIndex\",\"_type\":\"Flutschfinger\"}}\n{\"Eis\":\"am Stiel\"}\n")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.setBodyEncoding("UTF-8")).thenReturn(boundRequestBuilderMock);
        when(boundRequestBuilderMock.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "ok", "{\"errors\":true,\"items\":[{\"update\":{\"_index\":\"mytestindex\",\"_type\":\"product\",\"_id\":\"340891232\",\"status\":409}}]}")));

        testee.add(new IndexActionBuilder("someIndex").setOpType(IndexOpType.INDEX).setType("Flutschfinger").setSource(object("Eis", "am Stiel")));

        // when
        testee.execute();

        // then
        verify(asyncHttpClient).preparePost("/_bulk");
        verify(boundRequestBuilderMock).execute();
    }
}