package de.otto.elasticsearch.client.query;

import com.google.gson.JsonPrimitive;
import org.testng.annotations.Test;

import static de.otto.elasticsearch.client.request.GsonHelper.array;
import static de.otto.elasticsearch.client.request.GsonHelper.object;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class AndQueryBuilderTest {

    @Test
    public void shouldCreateAndQueryWithOneQuery() throws Exception {
        // given

        // when
        AndQueryBuilder andQueryBuilder = new AndQueryBuilder(new TermQueryBuilder("someName", new JsonPrimitive("someValue")));
        //then
        assertThat(andQueryBuilder.build(), is(object("and", array(object("term", object("someName", "someValue"))))));
    }

    @Test
    public void shouldCreateAndQueryWithMultipleQueries() throws Exception {
        // given

        // when
        AndQueryBuilder andQueryBuilder = new AndQueryBuilder(
                new TermQueryBuilder("someName0", new JsonPrimitive("someValue0")),
                new TermQueryBuilder("someName1", new JsonPrimitive("someValue1"))
        );

        //then
        assertThat(andQueryBuilder.build(), is(object("and", array(
                object("term", object("someName0", "someValue0")),
                object("term", object("someName1", "someValue1"))
        ))));
    }

    @Test
    public void shouldThrowExceptionIfFiltersIsMissing() {
        try {
            new AndQueryBuilder().build();

        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("missing property 'filters'"));
        }
    }
}
