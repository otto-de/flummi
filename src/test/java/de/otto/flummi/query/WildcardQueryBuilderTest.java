package de.otto.flummi.query;

import com.google.gson.JsonPrimitive;
import org.testng.annotations.Test;

import static de.otto.flummi.request.GsonHelper.object;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class WildcardQueryBuilderTest {

    @Test
    public void shouldCreateWildcardQuery() throws Exception {
        WildcardQueryBuilder wildcardQueryBuilder = new WildcardQueryBuilder("some*Name?", new JsonPrimitive(42));
        assertThat(wildcardQueryBuilder.build(), is(object("wildcard", object("some*Name?", 42))));
    }

    @Test
    public void shouldThrowExceptionIfNameIsMissing() throws Exception {
        try {
            new WildcardQueryBuilder(null, new JsonPrimitive(42)).build();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("missing property 'name'"));
        }
    }

    @Test
    public void shouldThrowExceptionIfNameIsEmpty() throws Exception {
        try {
            new WildcardQueryBuilder("", new JsonPrimitive(42)).build();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("missing property 'name'"));
        }
    }

    @Test
    public void shouldThrowExceptionIfValueIsMissing() throws Exception {
        try {
            new WildcardQueryBuilder("someName", null).build();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("missing property 'value'"));
        }
    }
}