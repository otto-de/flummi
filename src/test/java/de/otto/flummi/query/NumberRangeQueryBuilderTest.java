package de.otto.flummi.query;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.testng.annotations.Test;

import static de.otto.flummi.request.GsonHelper.object;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class NumberRangeQueryBuilderTest {

    @Test
    public void shouldAddLtFieldToQuery() throws Exception {
        // given
        // when
        NumberRangeQueryBuilder rangeQueryBuilder = new NumberRangeQueryBuilder("someField").lt(100);
        //then
        assertThat(rangeQueryBuilder.build(), is(object("range", object("someField", object("lt", 100)))));
    }

    @Test
    public void shouldAddGtFieldToQuery() throws Exception {
        // given
        // when
        NumberRangeQueryBuilder rangeQueryBuilder = new NumberRangeQueryBuilder("someField").gt(100);
        //then
        assertThat(rangeQueryBuilder.build(), is(object("range", object("someField", object("gt", 100)))));
    }

    @Test
    public void shouldAddLteFieldToQuery() throws Exception {
        // given
        // when
        NumberRangeQueryBuilder rangeQueryBuilder = new NumberRangeQueryBuilder("someField").lte(100);
        //then
        assertThat(rangeQueryBuilder.build(), is(object("range", object("someField", object("lte", 100)))));
    }

    @Test
    public void shouldAddGteFieldToQuery() throws Exception {
        // given
        // when
        NumberRangeQueryBuilder rangeQueryBuilder = new NumberRangeQueryBuilder("someField").gte(100);
        //then
        assertThat(rangeQueryBuilder.build(), is(object("range", object("someField", object("gte", 100)))));
    }

    @Test
    public void shouldAddGteAndLtFieldToQuery() throws Exception {
        // given
        // when
        NumberRangeQueryBuilder rangeQueryBuilder = new NumberRangeQueryBuilder("someField").gte(100).lt(200);
        //then
        JsonObject fieldObject = new JsonObject();
        fieldObject.add("gte", new JsonPrimitive(100));
        fieldObject.add("lt", new JsonPrimitive(200));
        assertThat(rangeQueryBuilder.build(), is(object("range", object("someField", fieldObject))));
    }

    @Test
    public void shouldThrowExceptionIfFromAndToAreMissing() throws Exception {
        // given
        // when
        try {
            new NumberRangeQueryBuilder("someField").build();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(),  is("from and to fields are missing"));
        }
        //then
    }

    @Test
    public void shouldThrowExceptionIfFieldNameIsEmpty() throws Exception {
        // given
        // when
        try {
            new NumberRangeQueryBuilder("").lt(100).build();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(),  is("fieldName is missing"));
        }
        //then
    }
}