package de.otto.flummi.domain.index;

public class IndexSettings {
    // static settings
    private Integer number_of_shards;
    // index.shard.check_on_startup ENUM?
    private IndexCodec codec;

    // dynamic settings
    private Integer number_of_replicas;

    //index.auto_expand_replicas, 0-5, 0-all, false

    //index.refresh_interval
    //index.max_result_window
    //index.max_rescore_window
    //index.max_refresh_listeners

    private IndexBlocks blocks;

    public void setNumberOfShards(Integer numberOfShards) {
        this.number_of_shards = numberOfShards;
    }

    public void setNumberOfReplicas(Integer numberOfReplicas) {
        this.number_of_replicas = numberOfReplicas;
    }
}
