package de.otto.flummi;

import de.otto.flummi.ClusterAdminClient;
 import org.elasticsearch.client.RestClient;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.mockito.Mockito.mock;

public class ClusterAdminClientTest {

    private ClusterAdminClient clusterAdminClient;
    private RestClient httpClient;

    @BeforeMethod
    public void setup() {
        httpClient = mock(RestClient.class);
        clusterAdminClient = new ClusterAdminClient(httpClient);
    }

    @Test
    public void shouldPrepareClusterHealth() {
        assertThat(clusterAdminClient.prepareHealth(), notNullValue());
    }

}