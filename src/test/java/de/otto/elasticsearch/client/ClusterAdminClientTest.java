package de.otto.elasticsearch.client;

import com.google.common.collect.ImmutableList;
import de.otto.elasticsearch.client.util.RoundRobinLoadBalancingHttpClient;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.mockito.Mockito.mock;

public class ClusterAdminClientTest {

    private ClusterAdminClient clusterAdminClient;
    private RoundRobinLoadBalancingHttpClient httpClient;

    @BeforeMethod
    public void setup() {
        httpClient = mock(RoundRobinLoadBalancingHttpClient.class);
        clusterAdminClient = new ClusterAdminClient(httpClient);
    }

    @Test
    public void shouldPrepareClusterHealth() {
        assertThat(clusterAdminClient.prepareHealth(), notNullValue());
    }

}