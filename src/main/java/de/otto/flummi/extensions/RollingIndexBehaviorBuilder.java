package de.otto.flummi.extensions;

import de.otto.flummi.IndicesAdminClient;

import java.util.function.Function;

public class RollingIndexBehaviorBuilder {
    private final IndicesAdminClient client;
    private final String aliasName;
    private final String indexPrefixName;
    private final int survivor;
    private Function<String, String> indexNameFunction = (s) -> s + "_" + System.currentTimeMillis();

    private RollingIndexBehaviorBuilder(IndicesAdminClient client, String aliasName, String indexPrefixName, int survivor) {
        this.client = client;
        this.aliasName = aliasName;
        this.indexPrefixName = indexPrefixName;
        this.survivor = survivor;
    }

    public static RollingIndexBehaviorBuilder builder(IndicesAdminClient client, String aliasName, String indexPrefixName, int survivor) {
        return new RollingIndexBehaviorBuilder(client, aliasName, indexPrefixName, survivor);
    }

    public RollingIndexBehaviorBuilder setIndexNameFunction(Function<String, String> indexNameFunction) {
        this.indexNameFunction = indexNameFunction;
        return this;
    }

    public RollingIndexBehavior build() {
        return new RollingIndexBehavior(client, aliasName, indexPrefixName, survivor, indexNameFunction);
    }
}