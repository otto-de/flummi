package de.otto.flummi;

public class ClusterHealthResponse {
    private ClusterHealthStatus status;
    private String cluster_name;
    private boolean timed_out;

    public ClusterHealthResponse(ClusterHealthStatus status, String cluster_name, boolean timed_out) {
        this.status = status;
        this.cluster_name = cluster_name;
        this.timed_out = timed_out;
    }

    public String getCluster_name() {
        return cluster_name;
    }

    public ClusterHealthStatus getStatus() {
        return status;
    }

    public boolean isTimedOut() {
        return timed_out;
    }
}
