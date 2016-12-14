package de.otto.flummi.domain.index;

import com.google.gson.Gson;
import org.testng.annotations.Test;

import static de.otto.flummi.domain.index.IndexBuilder.index;
import static org.testng.Assert.assertEquals;

public class IndexBuilderTest {

    @Test
    public void shouldCreateProperJson() throws Exception {
        // given
        IndexBuilder builder = index()
                .withNumberOfReplicas(2)
                .withNumberOfShards(3);

        // when
        String result = new Gson().toJson(builder.build());

        // then
        assertEquals(result, "{\"settings\":{\"index\":{\"number_of_shards\":3,\"number_of_replicas\":2}}}");
    }


}

