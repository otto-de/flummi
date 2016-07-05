package de.otto.flummi.response;


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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Bucket bucket = (Bucket) o;

        if (key != null ? !key.equals(bucket.key) : bucket.key != null) return false;
        return docCount != null ? docCount.equals(bucket.docCount) : bucket.docCount == null;

    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (docCount != null ? docCount.hashCode() : 0);
        return result;
    }
}
