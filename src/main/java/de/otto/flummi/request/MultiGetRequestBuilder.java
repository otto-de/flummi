package de.otto.flummi.request;

import com.google.gson.*;
import de.otto.flummi.RequestBuilderUtil;
import de.otto.flummi.response.MultiGetRequestDocument;
import de.otto.flummi.response.MultiGetResponse;
import de.otto.flummi.response.MultiGetResponseDocument;
import de.otto.flummi.util.HttpClientWrapper;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Response;
import org.slf4j.Logger;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static de.otto.flummi.RequestBuilderUtil.toHttpServerErrorException;
import static de.otto.flummi.request.GsonHelper.array;
import static de.otto.flummi.request.RequestConstants.APPL_JSON;
import static de.otto.flummi.request.RequestConstants.CONTENT_TYPE;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.slf4j.LoggerFactory.getLogger;

public class MultiGetRequestBuilder implements RequestBuilder<MultiGetResponse> {

    private final String[] indices;
    private final Gson gson;
    private final HttpClientWrapper httpClient;
    private String[] types;
    private Integer timeoutMillis;
    private List<MultiGetRequestDocument> documents;

    public static final Logger LOG = getLogger(MultiGetRequestBuilder.class);

    public MultiGetRequestBuilder(HttpClientWrapper httpClient, String... indices) {
        this.gson = new Gson();

        this.httpClient = httpClient;
        this.indices = indices;
    }

    public MultiGetRequestBuilder setRequestDocuments(List<MultiGetRequestDocument> requestDocument) {
        this.documents = requestDocument;
        return this;
    }

    public MultiGetRequestBuilder setTypes(String... types) {
        this.types = types;
        return this;
    }

    public MultiGetRequestBuilder setTimeoutMillis(Integer timeoutMillis) {
        this.timeoutMillis = timeoutMillis;
        return this;
    }

    @Override
    public MultiGetResponse execute() {
        try {
            String url = RequestBuilderUtil.buildUrl(indices, types, "_mget");
            JsonObject body = new JsonObject();
            if (documents != null) {
                body.add("docs", array(documents.stream().map(d -> create(d)).collect(toList())));
            }
            BoundRequestBuilder boundRequestBuilder = httpClient
                    .preparePost(url)
                    .setCharset(Charset.forName("UTF-8"));
            if (timeoutMillis != null) {
                boundRequestBuilder.setRequestTimeout(timeoutMillis);
            }
            long start = System.currentTimeMillis();
            Response response = boundRequestBuilder.setBody(gson.toJson(body))
                    .addHeader(CONTENT_TYPE, APPL_JSON)
                    .execute()
                    .get();

            long tookInMillis = System.currentTimeMillis() - start;
            //Did not find an entry
            if (response.getStatusCode() == 404) {
                return new MultiGetResponse(emptyList(), tookInMillis);
            }

            //Server Error
            if (response.getStatusCode() >= 300) {
                throw toHttpServerErrorException(response);
            }

            JsonObject jsonObject = gson.fromJson(response.getResponseBody(), JsonObject.class);
            JsonArray docs = jsonObject.get("docs").getAsJsonArray();

            List<MultiGetResponseDocument> documents = new ArrayList<>();
            for (JsonElement doc : docs) {
                JsonObject jsonDoc = doc.getAsJsonObject();
                String id = jsonDoc.get("_id").getAsString();
                JsonObject source = new JsonObject();
                boolean found = jsonDoc.get("found").getAsBoolean();
                if (found) {
                    source = jsonDoc.get("_source").getAsJsonObject();
                }
                documents.add(new MultiGetResponseDocument(id, found, source));
            }

            return new MultiGetResponse(documents, tookInMillis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private JsonObject create(MultiGetRequestDocument multiGetRequestDocument) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.add("_id", new JsonPrimitive(multiGetRequestDocument.getId()));
        if (multiGetRequestDocument.getType() != null) {
            jsonObject.add("_type", new JsonPrimitive(multiGetRequestDocument.getType()));
        }
        if (multiGetRequestDocument.getIndex() != null) {
            jsonObject.add("_index", new JsonPrimitive(multiGetRequestDocument.getIndex()));
        }
        if (multiGetRequestDocument.getFields() != null && multiGetRequestDocument.getFields().length > 0) {
            List<JsonElement> fieldList = new ArrayList<>();
            for (String field : multiGetRequestDocument.getFields()) {
                fieldList.add(new JsonPrimitive(field));

            }
            jsonObject.add("fields", array(fieldList));
        }
        return jsonObject;
    }

}
