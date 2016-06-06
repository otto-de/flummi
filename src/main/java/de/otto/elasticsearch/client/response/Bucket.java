package de.otto.elasticsearch.client.response;


public class Bucket {
    private String key;
    private Long docCount;

    public Bucket(String key, Long docCount) {
        this.key = key;
        this.docCount = docCount;
    }

    public String getKey() {
        return key;
    }

    public Long getDocCount() {
        return docCount;
    }

}
