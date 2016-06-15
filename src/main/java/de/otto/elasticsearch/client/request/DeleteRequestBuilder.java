package de.otto.elasticsearch.client.request;

import com.google.common.collect.ImmutableList;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Response;
import de.otto.elasticsearch.client.RequestBuilderUtil;
import de.otto.elasticsearch.client.util.RoundRobinLoadBalancingHttpClient;
import de.otto.elasticsearch.client.util.StringUtils;
import org.slf4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.concurrent.ExecutionException;

import static de.otto.elasticsearch.client.RequestBuilderUtil.buildUrl;
import static org.slf4j.LoggerFactory.getLogger;

public class DeleteRequestBuilder {
    private final RoundRobinLoadBalancingHttpClient httpClient;
    private String indexName;
    private String documentType;
    private String id;

    public static final Logger LOG = getLogger(DeleteRequestBuilder.class);

    public DeleteRequestBuilder(RoundRobinLoadBalancingHttpClient httpClient) {
        this.httpClient = httpClient;
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
            Response response = httpClient.prepareDelete(buildUrl(indexName, documentType, URLEncoder.encode(id, "UTF-8")))
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
            throw new RuntimeException(e);
        }
    }
}
