package de.otto.elasticsearch.client.request;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import de.otto.elasticsearch.client.ClusterHealthResponse;
import de.otto.elasticsearch.client.ClusterHealthStatus;
import de.otto.elasticsearch.client.InvalidElasticsearchResponseException;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static de.otto.elasticsearch.client.RequestBuilderUtil.toHttpServerErrorException;
import static org.slf4j.LoggerFactory.getLogger;

public class ClusterHealthRequestBuilder {
    private final AsyncHttpClient asyncHttpClient;
    private final ImmutableList<String> hosts;
    private final int hostIndexOfNextRequest;
    private final String[] indexNames;
    private final Gson gson;
    private boolean waitForYellowStatus;
    private Long timeout;

    public static final Logger LOG = getLogger(ClusterHealthRequestBuilder.class);

    public ClusterHealthRequestBuilder(AsyncHttpClient asyncHttpClient, ImmutableList<String> hosts, int hostIndexOfNextRequest, String[] indexNames) {
        this.asyncHttpClient = asyncHttpClient;
        this.hosts = hosts;
        this.hostIndexOfNextRequest = hostIndexOfNextRequest;
        this.indexNames = indexNames;
        this.gson = new Gson();
    }

    public ClusterHealthRequestBuilder setWaitForYellowStatus() {
        this.waitForYellowStatus = true;
        return this;
    }

    public ClusterHealthResponse execute() {
        for (int i = hostIndexOfNextRequest, count = 0; count < hosts.size(); i = (i + 1) % hosts.size()) {
            try {
                StringBuilder url = new StringBuilder("http://").append(hosts.get(i)).append("/_cluster/health");
                if (indexNames != null) {
                    url.append("/").append(String.join(",", indexNames));
                }
                AsyncHttpClient.BoundRequestBuilder requestBuilder = asyncHttpClient.prepareGet(url.toString());
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

    public ClusterHealthRequestBuilder setTimeout(long timeout) {
        this.timeout = timeout;
        return this;
    }
}
