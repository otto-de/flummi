package de.otto.flummi.request;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import de.otto.flummi.ClusterHealthResponse;
import de.otto.flummi.ClusterHealthStatus;
import de.otto.flummi.InvalidElasticsearchResponseException;
import de.otto.flummi.util.HttpClientWrapper;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static de.otto.flummi.RequestBuilderUtil.toHttpServerErrorException;
import static org.slf4j.LoggerFactory.getLogger;

public class ClusterHealthRequestBuilder implements RequestBuilder<ClusterHealthResponse> {
    private final String[] indexNames;
    private final Gson gson;
    private boolean waitForYellowStatus;
    private Long timeout;

    public static final Logger LOG = getLogger(ClusterHealthRequestBuilder.class);
    private HttpClientWrapper httpClient;


    public ClusterHealthRequestBuilder(HttpClientWrapper httpClient, String... indexNames) {
        this.httpClient = httpClient;
        this.indexNames = indexNames;
        this.gson = new Gson();
    }

    public ClusterHealthRequestBuilder setWaitForYellowStatus() {
        this.waitForYellowStatus = true;
        return this;
    }

    public ClusterHealthResponse execute() {
        try {
            StringBuilder url = new StringBuilder("/_cluster/health");
            if (indexNames != null) {
                url.append("/").append(String.join(",", indexNames));
            }
            AsyncHttpClient.BoundRequestBuilder requestBuilder = httpClient.prepareGet(url.toString());
            if (waitForYellowStatus) {
                requestBuilder.addQueryParam("wait_for_status", "yellow");
            }
            if (timeout != null) {
                requestBuilder.addQueryParam("timeout", timeout + "ms");
            }
            Response response = requestBuilder.execute().get();
            if (response.getStatusCode() >= 300) {
                throw toHttpServerErrorException(response);
            }
            JsonObject jsonResponse = gson.fromJson(response.getResponseBody(), JsonObject.class);
            if (jsonResponse.get("status") == null) {
                throw new InvalidElasticsearchResponseException("Missing response field: status");
            }
            if (jsonResponse.get("cluster_name") == null) {
                throw new InvalidElasticsearchResponseException("Missing response field: cluster_name");
            }
            if (jsonResponse.get("timed_out") == null) {
                throw new InvalidElasticsearchResponseException("Missing response field: timed_out");
            }

            ClusterHealthResponse clusterHealthResponse = new ClusterHealthResponse(ClusterHealthStatus.valueOf(jsonResponse.get("status").getAsString().toUpperCase()), jsonResponse.get("cluster_name").getAsString(), jsonResponse.get("timed_out").getAsBoolean());

            if (clusterHealthResponse.isTimedOut()) {
                throw new InvalidElasticsearchResponseException("Timed out waiting for yellow cluster status");
            }
            return clusterHealthResponse;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public ClusterHealthRequestBuilder setTimeout(long timeout) {
        this.timeout = timeout;
        return this;
    }
}
