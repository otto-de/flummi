package de.otto.elasticsearch.client.aggregations;

import com.google.gson.JsonObject;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class RangeBuilderTest {
    @Test
    public void shouldBuildAggregation() throws Exception {
        JsonObject rangeAggregation = new RangeBuilder("hans-peter")
                .field("Preis")
                .addRange("billig", 0, 100)
                .addRange("teuer", 101, 200).build();

        assertThat(rangeAggregation.toString(), is("{" +
                "\"range\":{" +
                "\"field\":\"Preis\"," +
                "\"ranges\":[" +
                "{\"key\":\"billig\",\"from\":0.0,\"to\":100.0}," +
                "{\"key\":\"teuer\",\"from\":101.0,\"to\":200.0}" +
                "]" +
                "}" +
                "}"));
    }

    @Test
    public void shouldBuildAggregationWithUnboundedRanges() throws Exception {
        JsonObject rangeAggregation = new RangeBuilder("hans-peter")
                .field("Preis")
                .addUnboundedTo("billig", 100)
                .addUnboundedFrom("teuer", 101).build();

        assertThat(rangeAggregation.toString(), is("{" +
                "\"range\":{" +
                "\"field\":\"Preis\"," +
                "\"ranges\":[" +
                "{\"key\":\"billig\",\"to\":100.0}," +
                "{\"key\":\"teuer\",\"from\":101.0}" +
                "]" +
                "}" +
                "}"));
    }
}