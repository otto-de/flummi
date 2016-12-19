package de.otto.flummi.extensions;

import com.google.gson.JsonObject;
import de.otto.flummi.IndicesAdminClient;

import java.util.Comparator;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.stream.Collectors.toSet;

public class RollingIndexBehavior {

    private final IndicesAdminClient client;
    private final String aliasName;
    private final String indexPrefixName;
    private final int survivor;
    private final Function<String, String> indexNameFunction;

    public RollingIndexBehavior(IndicesAdminClient client, String aliasName, String indexPrefixName, int survivor, Function<String, String> indexNameFunction) {
        this.client = client;
        this.aliasName = aliasName;
        this.indexPrefixName = indexPrefixName;
        this.survivor = survivor;
        this.indexNameFunction = indexNameFunction;
    }

    public RollingIndexBehavior(IndicesAdminClient client, String aliasName, String indexPrefixName, int survivor) {
        this(client, aliasName, indexPrefixName, survivor, (s) -> s+"_"+System.currentTimeMillis());
    }

    public String createNewIndex(JsonObject settings, JsonObject mappings) {
        String indexName = newIndexName();
        client.prepareCreate(indexName)
                .setSettings(settings)
                .setMappings(mappings)
                .execute();
        return indexName;
    }

    public String createNewIndex() {
        String indexName = newIndexName();
        client.prepareCreate(indexName).execute();
        return indexName;
    }

    public void abort(String newIndexName) {
        client.prepareDelete(newIndexName).execute();
    }

    public Set<String> commit(String name) {
        client.pointAliasToCurrentIndex(aliasName, name);
        return this.deleteOldIndices(aliasName, indexPrefixName, survivor);
    }

    private String newIndexName() {
        return indexNameFunction.apply(indexPrefixName);
    }

    Set<String> deleteOldIndices(String alias, String prefix, int survivor) {
        if(survivor < 1) {
            throw new IllegalArgumentException("must have one survivor");
        }

        Optional<String> aliasToIndex = client.getIndexNameForAlias(alias);
        Set<String> names =
                client.getAllIndexNames()
                        .stream()
                        .filter(startsWith(prefix))
                        .sorted(Comparator.reverseOrder()) // TODO: here we should have Index objects and sort by created date value
                        .skip(survivor)
                        .filter(skipAlias(aliasToIndex))
                        .collect(toSet());
        client.prepareDelete(names.stream()).execute();
        return names;
    }

    private static Predicate<? super String> startsWith(String prefix) {
        return (s) -> s.startsWith(prefix);
    }

    private static Predicate<String> skipAlias(Optional<String> indexName) {
        return (s) -> !(indexName.isPresent() && s.equals(indexName.get()));
    }
}
