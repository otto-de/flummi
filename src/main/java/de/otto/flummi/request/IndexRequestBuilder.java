package de.otto.flummi.request;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.ning.http.client.AsyncHttpClient;
import org.elasticsearch.client.Response;
import de.otto.flummi.domain.index.Index;
 import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.concurrent.ExecutionException;

import static de.otto.flummi.RequestBuilderUtil.buildUrl;
import static de.otto.flummi.RequestBuilderUtil.toHttpServerErrorException;
import static org.slf4j.LoggerFactory.getLogger;

public class IndexRequestBuilder implements RequestBuilder<Void> {
    private final Gson gson;
    private JsonPrimitive id;
    private String indexName;
    private String documentType;
    @Deprecated
    private JsonObject source;
    private String parent;

    public static final Logger LOG = getLogger(IndexRequestBuilder.class);
    private RestClient httpClient;
    private Index index;

    public IndexRequestBuilder(RestClient httpClient) {
        this.httpClient = httpClient;
        this.gson = new Gson();
    }

    public IndexRequestBuilder setId(String id) {
        this.id = new JsonPrimitive(id);
        return this;
    }

    public IndexRequestBuilder setId(int id) {
        this.id = new JsonPrimitive(id);
        return this;
    }

    public IndexRequestBuilder setIndexName(String indexName) {
        this.indexName = indexName;
        return this;
    }

    public IndexRequestBuilder setDocumentType(String documentType) {
        this.documentType = documentType;
        return this;
    }

    @Deprecated
    public IndexRequestBuilder setSource(JsonObject source) {
        this.source = source;
        return this;
    }

    public IndexRequestBuilder setIndex(Index index) {
        this.index = index;
        return this;
    }

    public IndexRequestBuilder setParent(String parent) {
        this.parent = parent;
        return this;
    }

    @Override
    public Void execute() {
        if (source == null) {
            if (index == null) {
                throw new IllegalStateException("either source or indexSettings must exist");
            }
        } else {
            if (index != null) {
                throw new IllegalStateException("either source or indexSettings is allowed to specified");
            }
        }
        try {
            AsyncHttpClient.BoundRequestBuilder reqBuilder;
            if (id != null) {
                String url = buildUrl(indexName, documentType, URLEncoder.encode(id.getAsString(), "UTF-8"));
                reqBuilder = httpClient.preparePut(url);
            } else {
                String url = buildUrl(indexName, documentType);
                reqBuilder = httpClient.preparePost(url);
            }
            if (parent != null) {
                reqBuilder.addQueryParam("parent", parent);
            }

            String body = createBody();
            Response response = reqBuilder.setBody(body).setBodyEncoding("UTF-8").execute().get();
            if (response.getStatusCode() >= 300) {
                throw toHttpServerErrorException(response);
            }
            return null;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private String createBody() {
        if (source != null) {
            return gson.toJson(source);
        }
        if (index != null) {
            return gson.toJson(index);
        }
        throw new IllegalStateException();


    }

}
