package de.otto.flummi.request;

import com.google.gson.*;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import de.otto.flummi.RequestBuilderUtil;
import de.otto.flummi.response.AnalyzeResponse;
import de.otto.flummi.response.Token;
import de.otto.flummi.util.HttpClientWrapper;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static de.otto.flummi.RequestBuilderUtil.toHttpServerErrorException;
import static org.slf4j.LoggerFactory.getLogger;

public class AnalyzeRequestBuilder implements RequestBuilder<AnalyzeResponse> {
    private HttpClientWrapper httpClient;
    private final Gson gson;
    private String indexName;
    private final String text;
    private String tokenizer;
    private String analyzer;
    private String field;
    private JsonArray filters;
    private JsonArray characterFilters;

    public static final Logger LOG = getLogger(AnalyzeRequestBuilder.class);

    public AnalyzeRequestBuilder(HttpClientWrapper httpClient, String text) {
        this.httpClient = httpClient;
        this.text = text;
        this.gson = new Gson();
    }

    @Override
    public AnalyzeResponse execute() {
        JsonObject body = new JsonObject();
        try {
            String url = buildUrl();

            if (text != null) {
                body.add("text", new JsonPrimitive(text));
            }

            if (analyzer != null) {
                body.add("analyzer", new JsonPrimitive(analyzer));
            }

            if (tokenizer != null) {
                body.add("tokenizer", new JsonPrimitive(tokenizer));
            }

            if (field != null) {
                body.add("field", new JsonPrimitive(field));
            }

            if (filters != null) {
                body.add("filter", filters);
            }

            if (characterFilters != null) {
                body.add("char_filter", characterFilters);
            }

            Response response = httpClient
                    .prepareGet(url)
                    .setBodyEncoding("UTF-8")
                    .setBody(gson.toJson(body))
                    .execute()
                    .get();

            if (response.getStatusCode() != 200) {
                throw toHttpServerErrorException(response);
            }

            JsonObject jsonResponse = gson.fromJson(response.getResponseBody(), JsonObject.class);
            AnalyzeResponse.Builder analyzeResponse = parseResponse(jsonResponse);

            return analyzeResponse.build();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private String buildUrl() {
        StringBuilder urlBuilder = new StringBuilder();
        if (indexName != null) {
            urlBuilder.append("/").append(indexName);
        }
        urlBuilder.append("/").append("_analyze");
        return urlBuilder.toString();
    }

    public AnalyzeRequestBuilder setTokenizer(String tokenizer) {
        this.tokenizer = tokenizer;
        return this;
    }

    public AnalyzeRequestBuilder setAnalyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    public AnalyzeRequestBuilder setIndexName(String indexName) {
        this.indexName = indexName;
        return this;
    }

    public AnalyzeRequestBuilder appendFilter(String filter) {
        if (filters == null) {
            filters = new JsonArray();
        }

        filters.add(new JsonPrimitive(filter));
        return this;
    }

    public AnalyzeRequestBuilder appendCharacterFilter(String characterFilter) {
        if (characterFilters == null) {
            characterFilters = new JsonArray();
        }

        characterFilters.add(new JsonPrimitive(characterFilter));
        return this;
    }

    public AnalyzeRequestBuilder setField(String field) {
        this.field = field;
        return this;
    }

    public static AnalyzeResponse.Builder parseResponse(JsonObject jsonObject) {
        AnalyzeResponse.Builder analyzeResponse = AnalyzeResponse.builder();
        JsonArray tokensArray = jsonObject.get("tokens").getAsJsonArray();

        List<Token> tokens = new ArrayList<>();
        for (JsonElement element : tokensArray) {
            JsonObject asJsonObject = element.getAsJsonObject();
            String token = asJsonObject.get("token").getAsString();
            String type = asJsonObject.get("type").getAsString();
            Integer position = asJsonObject.get("position").getAsInt();
            Integer startOffset = asJsonObject.get("start_offset").getAsInt();
            Integer endOffset = asJsonObject.get("end_offset").getAsInt();
            Token t = new Token(token, type, position, startOffset, endOffset);
            tokens.add(t);
        }

        analyzeResponse.setHits(tokens);

        return analyzeResponse;
    }
}
