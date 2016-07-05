package de.otto.flummi;

import de.otto.flummi.ClusterAdminClient;
import de.otto.flummi.util.HttpClientWrapper;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.mockito.Mockito.mock;

public class ClusterAdminClientTest {

    private ClusterAdminClient clusterAdminClient;
    private HttpClientWrapper httpClient;

    @BeforeMethod
    public void setup() {
        httpClient = mock(HttpClientWrapper.class);
        clusterAdminClient = new ClusterAdminClient(httpClient);
    }

    @Test
    public void shouldPrepareClusterHealth() {
        assertThat(clusterAdminClient.prepareHealth(), notNullValue());
    }

}