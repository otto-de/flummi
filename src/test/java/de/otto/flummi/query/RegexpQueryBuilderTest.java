package de.otto.flummi.query;

import com.google.gson.JsonPrimitive;
import org.testng.annotations.Test;

import static de.otto.flummi.request.GsonHelper.object;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class RegexpQueryBuilderTest {

    @Test
    public void shouldCreateRegexpQuery() throws Exception {
        RegexpQueryBuilder regexpQueryBuilder = new RegexpQueryBuilder("some.*N", new JsonPrimitive(42));
        assertThat(regexpQueryBuilder.build(), is(object("regexp", object("some.*N", 42))));
    }

    @Test
    public void shouldThrowExceptionIfNameIsMissing() throws Exception {
        try {
            new RegexpQueryBuilder(null, new JsonPrimitive(42)).build();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("missing property 'name'"));
        }
    }

    @Test
    public void shouldThrowExceptionIfNameIsEmpty() throws Exception {
        try {
            new RegexpQueryBuilder("", new JsonPrimitive(42)).build();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("missing property 'name'"));
        }
    }

    @Test
    public void shouldThrowExceptionIfValueIsMissing() throws Exception {
        try {
            new RegexpQueryBuilder("someName", null).build();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("missing property 'value'"));
        }
    }
}