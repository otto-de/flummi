package de.otto.elasticsearch.client.request;

import com.google.common.collect.ImmutableList;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import de.otto.elasticsearch.client.RequestBuilderUtil;
import de.otto.elasticsearch.client.util.StringUtils;
import org.slf4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.concurrent.ExecutionException;

import static de.otto.elasticsearch.client.RequestBuilderUtil.buildUrl;
import static org.slf4j.LoggerFactory.getLogger;

public class DeleteRequestBuilder {
    private final AsyncHttpClient asyncHttpClient;
    private final ImmutableList<String> hosts;
    private final int hostIndexOfNextRequest;
    private String indexName;
    private String documentType;
    private String id;

    public static final Logger LOG = getLogger(DeleteRequestBuilder.class);

    public DeleteRequestBuilder(AsyncHttpClient asyncHttpClient, ImmutableList<String> hosts, int hostIndexOfNextRequest) {
        this.asyncHttpClient = asyncHttpClient;
        this.hosts = hosts;
        this.hostIndexOfNextRequest = hostIndexOfNextRequest;
    }

    public DeleteRequestBuilder setIndexName(final String indexName) {
        this.indexName = indexName;
        return this;
    }

    public DeleteRequestBuilder setDocumentType(final String documentType) {
        this.documentType = documentType;
        return this;
    }

    public DeleteRequestBuilder setId(String id) {
        this.id = id;
        return this;
    }

    public void execute() {
        for (int i = hostIndexOfNextRequest, count = 0; count < hosts.size(); i = (i + 1) % hosts.size()) {
            try {
                if (StringUtils.isEmpty(indexName)) {
                    throw new RuntimeException("missing property 'indexName'");
                }
                if (StringUtils.isEmpty(documentType)) {
                    throw new RuntimeException("missing property 'type'");
                }
                if (StringUtils.isEmpty(id)) {
                    throw new RuntimeException("missing property 'id'");
                }
                Response response = asyncHttpClient.prepareDelete(buildUrl(hosts.get(i), indexName, documentType, URLEncoder.encode(id, "UTF-8")))
                        .execute().get();
                if (response.getStatusCode() >= 300) {
                    throw RequestBuilderUtil.toHttpServerErrorException(response);
                }
                return;
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
