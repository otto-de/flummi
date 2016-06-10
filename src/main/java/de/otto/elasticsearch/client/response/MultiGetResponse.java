package de.otto.elasticsearch.client.response;

import java.util.List;

public class MultiGetResponse {

    private final List<MultiGetResponseDocument> multiGetResponseDocuments;
    private final long tookInMillis;

    public MultiGetResponse(List<MultiGetResponseDocument> multiGetResponseDocuments, long tookInMillis) {
        this.multiGetResponseDocuments = multiGetResponseDocuments;
        this.tookInMillis = tookInMillis;
    }

    public List<MultiGetResponseDocument> getMultiGetResponseDocuments() {
        return multiGetResponseDocuments;
    }
}
