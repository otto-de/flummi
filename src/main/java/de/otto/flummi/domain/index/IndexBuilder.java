package de.otto.flummi.domain.index;

public class IndexBuilder {

    private Index index;

    public static IndexBuilder index() {
        return new IndexBuilder();
    }

    private IndexBuilder() {
        index = new Index();
    }

    public Index build() {
        if (index == null) {
            throw new IllegalStateException();
        }
        Index resValue = index;
        index = null;
        return resValue;
    }

    public IndexBuilder withNumberOfShards(Integer numberOfShards) {
        index.getSettings().getIndex().setNumberOfShards(numberOfShards);
        return this;
    }

    public IndexBuilder withNumberOfReplicas(Integer numberOfReplicas) {
        index.getSettings().getIndex().setNumberOfReplicas(numberOfReplicas);
        return this;
    }
}
