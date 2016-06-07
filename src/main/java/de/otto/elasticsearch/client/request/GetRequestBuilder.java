package de.otto.elasticsearch.client.request;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import de.otto.elasticsearch.client.RequestBuilderUtil;
import de.otto.elasticsearch.client.response.GetResponse;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URLEncoder;
import java.util.concurrent.ExecutionException;

import static de.otto.elasticsearch.client.RequestBuilderUtil.toHttpServerErrorException;
import static org.slf4j.LoggerFactory.getLogger;

public class GetRequestBuilder implements RequestBuilder<GetResponse> {
    private final AsyncHttpClient client;
    private final ImmutableList<String> hosts;
    private final int hostIndexOfNextRequest;
    private final String indexName;
    private final String documentType;
    private final String id;
    private final Gson gson;

    public static final Logger LOG = getLogger(GetRequestBuilder.class);

    public GetRequestBuilder(AsyncHttpClient client, ImmutableList<String> hosts, int hostIndexOfNextRequest, String indexName, String documentType, String id) {
        this.client = client;
        this.hosts = hosts;
        this.hostIndexOfNextRequest = hostIndexOfNextRequest;
        this.indexName = indexName;
        this.documentType = documentType;
        this.id = id;
        this.gson = new Gson();
    }

    @Override
    public GetResponse execute() {
        for (int i = hostIndexOfNextRequest, count = 0; count < hosts.size(); i = (i + 1) % hosts.size()) {
            try {
                String url = RequestBuilderUtil.buildUrl(hosts.get(i), indexName, documentType, URLEncoder.encode(id, "UTF-8"));
                Response response = client.prepareGet(url).execute().get();
                if (response.getStatusCode() >= 300 && 404 != response.getStatusCode()) {
                    throw toHttpServerErrorException(response);
                }

                if (404 == response.getStatusCode()) {
                    return new GetResponse(false, null, id);
                }
                String jsonString = response.getResponseBody();
                JsonObject responseObject = gson.fromJson(jsonString, JsonObject.class);
                return new GetResponse(true, responseObject != null && responseObject.get("_source") != null
                        ? responseObject.get("_source").getAsJsonObject()
                        : null, responseObject.get("_id").getAsString());

            } catch (IOException e) {
                throw new UncheckedIOException(e);
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
