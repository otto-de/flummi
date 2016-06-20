package de.otto.elasticsearch.client.query.sort;

import com.google.gson.JsonObject;
import de.otto.elasticsearch.client.SortOrder;
import de.otto.elasticsearch.client.query.QueryBuilders;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class FieldSortBuilderTest {

    private FieldSortBuilder testee = new FieldSortBuilder("field.name");

    @Test
    public void testName() throws Exception {
        JsonObject json = testee
                .setNestedFilter(QueryBuilders.matchAll())
                .setNestedPath("field")
                .setOrder(SortOrder.DESC)
                .setSortMode(SortMode.MEDIAN)
                .build();

        assertThat(json.toString(), is("{\"field.name\":" +
                "{" +
                "\"order\":\"desc\"," +
                "\"mode\":\"median\"," +
                "\"nested_filter\":{\"match_all\":{}}," +
                "\"nested_path\":\"field\"}" +
                "}"));
    }
}