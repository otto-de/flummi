package de.otto.flummi.query;

import com.google.gson.JsonObject;
import org.testng.annotations.Test;

import static de.otto.flummi.request.GsonHelper.object;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class HasParentQueryBuilderTest {

    @Test
    public void shouldBuildHasParentQuery() throws Exception {
        // when
        JsonObject queryAsJson = new HasParentQueryBuilder("someType", QueryBuilders.matchAll()).build();

        //then
        JsonObject query = object("type", "someType");
        query.add("query", object("match_all", new JsonObject()));
        assertThat(queryAsJson, is(
                object("has_parent",
                        query)));
    }


}