package de.otto.flummi.query;

import com.google.gson.JsonPrimitive;
import org.testng.annotations.Test;

import static de.otto.flummi.request.GsonHelper.object;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class TermQueryBuilderTest {

    @Test
    public void shouldCreateTermQuery() throws Exception {
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("someName", new JsonPrimitive(42));
        assertThat(termQueryBuilder.build(), is(object("term", object("someName", 42))));
    }

    @Test
    public void shouldThrowExceptionIfNameIsMissing() throws Exception {
        try {
            new TermQueryBuilder(null, new JsonPrimitive(42)).build();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("missing property 'name'"));
        }
    }

    @Test
    public void shouldThrowExceptionIfNameIsEmpty() throws Exception {
        try {
            new TermQueryBuilder("", new JsonPrimitive(42)).build();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("missing property 'name'"));
        }
    }

    @Test
    public void shouldThrowExceptionIfValueIsMissing() throws Exception {
        try {
            new TermQueryBuilder("someName", null).build();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("missing property 'value'"));
        }
    }
}