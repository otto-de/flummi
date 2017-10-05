package de.otto.flummi.request;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import de.otto.flummi.CompletedFuture;
import de.otto.flummi.MockResponse;
import de.otto.flummi.response.AnalyzeResponse;
import de.otto.flummi.response.HttpServerErrorException;
import de.otto.flummi.response.Token;
import de.otto.flummi.util.HttpClientWrapper;
import org.asynchttpclient.BoundRequestBuilder;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.Charset;
import java.util.ArrayList;

import static de.otto.flummi.request.GsonHelper.array;
import static de.otto.flummi.request.GsonHelper.object;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class AnalyzeRequestBuilderTest {
    @Mock
    HttpClientWrapper httpClient;

    @Mock
    BoundRequestBuilder boundRequestBuilder;

    AnalyzeRequestBuilder helloWorldAnalyzeRequestBuilder;

    @BeforeMethod
    public void setUp() throws Exception {
        initMocks(this);
        when(httpClient.prepareGet(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setCharset(Charset.forName("UTF-8"))).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.setBody(anyString())).thenReturn(boundRequestBuilder);
        when(boundRequestBuilder.execute()).thenReturn(
                new CompletedFuture<>(new MockResponse(200, "ok", helloWorldResponse.toString()))
        );
        helloWorldAnalyzeRequestBuilder = new AnalyzeRequestBuilder(httpClient, "Hello World");
    }

    private JsonObject helloWorldResponse = object(
            "tokens", array(
                    createTokenJsonObject("hello", "<ALPHANUM>", 0, 0, 5),
                    createTokenJsonObject("world", "<ALPHANUM>", 1, 6, 11)
            )
    );


    @Test
    public void shouldCorrectlyParseResponse() throws Exception {
        when(boundRequestBuilder.execute()).thenReturn(
                new CompletedFuture<>(new MockResponse(200, "ok", helloWorldResponse.toString()))
        );

        // when
        AnalyzeResponse analyzerResponse = helloWorldAnalyzeRequestBuilder.execute();

        // then
        final ArrayList<Token> expectedTokens = new ArrayList<>();
        expectedTokens.add(new Token("hello", "<ALPHANUM>", 0, 0, 5));
        expectedTokens.add(new Token("world", "<ALPHANUM>", 1, 6, 11));

        assertThat(analyzerResponse.getTokens(), is(expectedTokens));
        verify(boundRequestBuilder).execute();
    }

    @Test
    public void whenOnlyTextShouldTargetAnalyzeAndNotAddAdditionalParamsAndPreserveCase() throws Exception {
        helloWorldAnalyzeRequestBuilder.execute();

        verify(httpClient).prepareGet("/_analyze");
        verify(boundRequestBuilder).setBody(object("text", "Hello World").toString());
    }

    @Test
    public void withSpecifiedAnalyzerShouldAddItAsParam() throws Exception {
        helloWorldAnalyzeRequestBuilder.setAnalyzer("custom_one");

        // when
        helloWorldAnalyzeRequestBuilder.execute();

        // then
        verify(httpClient).prepareGet("/_analyze");
        verify(boundRequestBuilder).setBody(object(
                "text", "Hello World",
                "analyzer", "custom_one"
        ).toString());
    }

    @Test
    public void withSpecifiedTokenizerShouldAddItAsParam() throws Exception {
        helloWorldAnalyzeRequestBuilder.setTokenizer("keyword");

        // when
        helloWorldAnalyzeRequestBuilder.execute();

        // then
        verify(httpClient).prepareGet("/_analyze");
        verify(boundRequestBuilder).setBody(object(
                "text", "Hello World",
                "tokenizer", "keyword"
        ).toString());
    }

    @Test
    public void withSpecifiedFieldShouldUseItAsParam() throws Exception {
        helloWorldAnalyzeRequestBuilder.setField("myField");

        // when
        helloWorldAnalyzeRequestBuilder.execute();

        // then
        verify(httpClient).prepareGet("/_analyze");
        verify(boundRequestBuilder).setBody(object(
                "text", "Hello World",
                "field", "myField"
        ).toString());
    }

    @Test
    public void withSpecifiedFiltersShouldUseThemAndPerserveTheirOrder() throws Exception {
        helloWorldAnalyzeRequestBuilder
                .appendFilter("lowercase")
                .appendFilter("unique");

        // when
        helloWorldAnalyzeRequestBuilder.execute();

        // then
        verify(httpClient).prepareGet("/_analyze");
        verify(boundRequestBuilder).setBody(object(
                "text", new JsonPrimitive("Hello World"),
                "filter", array(new JsonPrimitive("lowercase"), new JsonPrimitive("unique"))
        ).toString());
    }

    @Test
    public void withSpecifiedCharacterFiltersShouldUseThemAndPerserveTheirOrder() throws Exception {
        helloWorldAnalyzeRequestBuilder
                .appendCharacterFilter("html_strip")
                .appendCharacterFilter("my_char_filter");

        // when
        helloWorldAnalyzeRequestBuilder.execute();

        // then
        verify(httpClient).prepareGet("/_analyze");
        verify(boundRequestBuilder).setBody(object(
                "text", new JsonPrimitive("Hello World"),
                "char_filter", array(new JsonPrimitive("html_strip"), new JsonPrimitive("my_char_filter"))
        ).toString());
    }

    @Test
    public void withEverythingShouldNotMissAnyParameter() throws Exception {
        helloWorldAnalyzeRequestBuilder
                .setIndexName("my-index")
                .setTokenizer("keyword")
                .setAnalyzer("custom_one")
                .setField("myField")
                .appendFilter("lowercase")
                .appendFilter("unique")
                .appendCharacterFilter("html_strip")
                .appendCharacterFilter("my_char_filter");

        // when
        helloWorldAnalyzeRequestBuilder.execute();

        // then
        JsonObject expectedBody = new JsonObject();
        expectedBody.add("text", new JsonPrimitive("Hello World"));
        expectedBody.add("analyzer", new JsonPrimitive("custom_one"));
        expectedBody.add("tokenizer", new JsonPrimitive("keyword"));
        expectedBody.add("field", new JsonPrimitive("myField"));
        expectedBody.add("filter", array(new JsonPrimitive("lowercase"), new JsonPrimitive("unique")));
        expectedBody.add("char_filter", array(new JsonPrimitive("html_strip"), new JsonPrimitive("my_char_filter")));
        verify(httpClient).prepareGet("/my-index/_analyze");
        verify(boundRequestBuilder).setBody(expectedBody.toString());
    }

    @Test
    public void withSpecifiedIndexNameShouldUseItInPath() throws Exception {
        helloWorldAnalyzeRequestBuilder
                .setIndexName("my-index");

        // when
        helloWorldAnalyzeRequestBuilder.execute();

        // then
        verify(httpClient).prepareGet("/my-index/_analyze");
        verify(boundRequestBuilder).setBody(object("text", "Hello World").toString());
    }

    @Test(expectedExceptions = HttpServerErrorException.class)
    public void shouldThrowWhenServerReturnsNon200StatusCode() throws Exception {
        when(boundRequestBuilder.execute()).thenReturn(new CompletedFuture<>(new MockResponse(404, "Not Found", object("status", new JsonPrimitive(404), "error", object()).toString())));

        // when
        try {
            helloWorldAnalyzeRequestBuilder.execute();
        }
        // then
        catch (HttpServerErrorException e) {
            assertThat(e.getStatusCode(), is(404));
            assertThat(e.getResponseBody(), is("{\"status\":404,\"error\":{}}"));
            throw e;
        }
    }

    private JsonObject createTokenJsonObject(String token, String type, Integer position, Integer startOffset, Integer endOffset) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.add("position", new JsonPrimitive(position));
        jsonObject.add("type", new JsonPrimitive(type));
        jsonObject.add("end_offset", new JsonPrimitive(endOffset));
        jsonObject.add("start_offset", new JsonPrimitive(startOffset));
        jsonObject.add("token", new JsonPrimitive(token));
        return jsonObject;
    }
}
