package de.otto.elasticsearch.client;

import com.google.common.collect.ImmutableList;
import com.ning.http.client.AsyncHttpClient;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.mockito.Mockito.mock;

public class ClusterAdminClientTest {

    private ClusterAdminClient clusterAdminClient;
    private AsyncHttpClient asyncHttpClient;
    private ImmutableList<String> HOSTS = ImmutableList.of("someHost:9200");

    @BeforeMethod
    public void setup() {
        asyncHttpClient = mock(AsyncHttpClient.class);
        clusterAdminClient = new ClusterAdminClient(asyncHttpClient, HOSTS, 0);
    }

    @Test
    public void shouldPrepareClusterHealth(){
        assertThat(clusterAdminClient.prepareHealth(), notNullValue());
    }

}