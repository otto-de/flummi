package de.otto.elasticsearch.client.response;

public class MultiGetRequestDocument {

    private final String id;
    private final String type;
    private final String index;
    private final String[] fields;

    private MultiGetRequestDocument(String id, String type, String index, String[] fields) {
        this.id = id;
        this.type = type;
        this.index = index;
        this.fields = fields;
    }

    public String getId() {
        return id;
    }

    public String getType() {
        return type;
    }

    public String getIndex() {
        return index;
    }

    public String[] getFields() {
        return fields;
    }

    public static Builder multiGetRequestDocumentBuilder() {
        return new Builder();
    }

    public static class Builder {
        private String id;
        private String type;
        private String index;
        private String[] fields;

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public Builder withType(String type) {
            this.type = type;
            return this;
        }

        public Builder withIndex(String index) {
            this.index = index;
            return this;
        }

        public Builder withFields(String[] fields) {
            this.fields = fields;
            return this;
        }

        public MultiGetRequestDocument build() {
            return new MultiGetRequestDocument(id, type, index, fields);
        }
    }

}
