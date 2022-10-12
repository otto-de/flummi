package de.otto.flummi.request;

import de.otto.flummi.CompletedFuture;
import de.otto.flummi.MockResponse;
import de.otto.flummi.util.HttpClientWrapper;
import org.asynchttpclient.BoundRequestBuilder;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.Charset;

import static de.otto.flummi.request.GsonHelper.object;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class UpdateIndexSettingsRequestBuilderTest {

    @Mock
    HttpClientWrapper httpClient;

    @Mock
    BoundRequestBuilder boundRequestBuilder;

    UpdateIndexSettingsRequestBuilder testee;

    @BeforeMethod
    public void setUp() throws Exception {
        initMocks(this);
        testee = new UpdateIndexSettingsRequestBuilder(httpClient, "someIndex");
    }

    @Test
    public void shouldExecuteCreateIndexRequestWithMappings() throws Exception {
        // given
        when(httpClient.preparePut("/someIndex/_settings")).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody(any(String.class))).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setCharset(Charset.forName("UTF-8"))).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture(new MockResponse(200, "ok", "{\"acknowledged\": true}")));
        when(boundRequestBuilder.addHeader(anyString(),anyString())).thenReturn(boundRequestBuilder);
        testee
                .setSettings(object("index", object("number_of_replicas", 1)));
        // when
        testee.execute();

        // then
        verify(httpClient).preparePut("/someIndex/_settings");
        verify(boundRequestBuilder).execute();
        verify(boundRequestBuilder).setBody("{\"index\":{\"number_of_replicas\":1}}");
    }
}
