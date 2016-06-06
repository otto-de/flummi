package de.otto.elasticsearch.client.request;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import org.slf4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.concurrent.ExecutionException;

import static de.otto.elasticsearch.client.RequestBuilderUtil.buildUrl;
import static de.otto.elasticsearch.client.RequestBuilderUtil.toHttpServerErrorException;
import static org.slf4j.LoggerFactory.getLogger;

public class IndexRequestBuilder implements RequestBuilder<Void> {
    private final AsyncHttpClient asyncHttpClient;
    private final ImmutableList<String> hosts;
    private final int hostIndexOfNextRequest;
    private final Gson gson;
    private JsonPrimitive id;
    private String indexName;
    private String documentType;
    private JsonObject source;
    private String parent;

    public static final Logger LOG = getLogger(IndexRequestBuilder.class);

    public IndexRequestBuilder(AsyncHttpClient asyncHttpClient, ImmutableList<String> hosts, int hostIndexOfNextRequest) {
        this.asyncHttpClient = asyncHttpClient;
        this.hosts = hosts;
        this.hostIndexOfNextRequest = hostIndexOfNextRequest;
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

    public IndexRequestBuilder setSource(JsonObject source) {
        this.source = source;
        return this;
    }

    public IndexRequestBuilder setParent(String parent) {
        this.parent = parent;
        return this;
    }

    @Override
    public Void execute() {
        for (int i = hostIndexOfNextRequest, count = 0; count < hosts.size(); i = (i + 1) % hosts.size()) {
            AsyncHttpClient.BoundRequestBuilder reqBuilder;
            try {
                if (id != null) {
                    String url = buildUrl(hosts.get(i), indexName, documentType, URLEncoder.encode(id.getAsString(), "UTF-8"));
                    reqBuilder = asyncHttpClient.preparePut(url);
                } else {
                    String url = buildUrl(hosts.get(i), indexName, documentType);
                    reqBuilder = asyncHttpClient.preparePost(url);
                }
                if(parent!=null) {
                    reqBuilder.addQueryParam("parent", parent);
                }
                Response response = reqBuilder.setBody(gson.toJson(source)).setBodyEncoding("UTF-8").execute().get();
                if (response.getStatusCode() >= 300) {
                    throw toHttpServerErrorException(response);
                }
                return null;
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                if (i == ((hostIndexOfNextRequest - 1) % hosts.size())) {
                    LOG.warn("Could not connect to host '" + hosts.get(i) + "'");
                    throw new RuntimeException(e);
                } else {
                    LOG.warn("Could not connect to host '" + hosts.get(i) + "'");
                }
            }
            count++;
        }
        throw new RuntimeException("Could not connect to cluster");
    }
}
