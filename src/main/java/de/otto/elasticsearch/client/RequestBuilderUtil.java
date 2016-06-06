package de.otto.elasticsearch.client;

import com.ning.http.client.Response;
import de.otto.elasticsearch.client.response.HttpServerErrorException;

import java.io.IOException;
import java.io.UncheckedIOException;

public class RequestBuilderUtil {

    public static final String[] EMPTY_ARRAY = new String[]{};

    public static String buildUrl(String baseUrl, String[] indexNames, String[] types, String operationOrId) {
        StringBuilder urlBuilder = new StringBuilder("http://" + baseUrl);
        if (indexNames != null && indexNames.length > 0) {
            urlBuilder.append("/").append(String.join(",", indexNames));
        }
        if (types != null && types.length > 0) {
            urlBuilder.append("/").append(String.join(",", types));
        }
        if (operationOrId != null) {
            final String[] splitBySlash = operationOrId.split("/");
            urlBuilder.append("/").append(splitBySlash[splitBySlash.length -1]);
        }
        return urlBuilder.toString();
    }

    public static HttpServerErrorException toHttpServerErrorException(Response response) {
        try {
            return new HttpServerErrorException(response.getStatusCode(), response.getStatusText() , new String(response.getResponseBodyAsBytes()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static String buildUrl(String host, String indexName) {
        return "http://" +
                host +
                "/" +
                indexName;
    }

    public static String buildUrl(String baseUrl, String indexName, String type, String operationOrId) {
        String[] types = type != null ? new String[]{type} : EMPTY_ARRAY;
        String[] indexNames = indexName != null ? new String[]{indexName} : EMPTY_ARRAY;
        return buildUrl(baseUrl, indexNames, types, operationOrId);
    }

    public static String buildUrl(String baseUrl, String indexName, String documentType) {
        return buildUrl(baseUrl, indexName, documentType, null);
    }
}
