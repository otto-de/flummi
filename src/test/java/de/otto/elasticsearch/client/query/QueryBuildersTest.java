package de.otto.elasticsearch.client.query;

import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import static de.otto.elasticsearch.client.request.GsonHelper.array;
import static de.otto.elasticsearch.client.request.GsonHelper.object;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class QueryBuildersTest {

    @Test
    public void shouldCreateFilteredQuery() {
        LocalDate now = LocalDate.now();
        JsonObject jsonObject = QueryBuilders.filteredQuery(new TermQueryBuilder("someName", new JsonPrimitive("someValue")), new DateRangeQueryBuilder("someDateField").lt(now).build()).build();
        JsonObject filteredObject = new JsonObject();
        filteredObject.add("query", object("term", object("someName", "someValue")));
        filteredObject.add("filter", object("range", object("someDateField", object("lt", new JsonPrimitive(now.format(DateTimeFormatter.ISO_DATE))))));
        assertThat(jsonObject, is(object("filtered", filteredObject)));
    }

    @Test
    public void shouldCreateTermsQuery() {
        TermsQueryBuilder termsQueryBuilder = QueryBuilders.termsQuery("someName", new JsonPrimitive("someValue"));
        assertThat(termsQueryBuilder.build(), is(object("terms", object("someName", "someValue"))));
    }

    @Test
    public void shouldCreateTermQuery() {
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("someName", "someValue");
        assertThat(termQueryBuilder.build(), is(object("term", object("someName", "someValue"))));
    }

    @Test
    public void shouldCreateTermQuery2() {
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("someName", new JsonPrimitive("someValue"));
        assertThat(termQueryBuilder.build(), is(object("term", object("someName", "someValue"))));
    }

    @Test
    public void shouldCreateNestedQuery() {
        LocalDate now = LocalDate.now();
        JsonObject jsonObject = QueryBuilders.nestedQuery("somePath", QueryBuilders.prefixFilter("someName", "somePrefix")).build();
        JsonObject nestedObject = new JsonObject();
        nestedObject.add("path", new JsonPrimitive("somePath"));
        nestedObject.add("filter", object("prefix", object("someName", "somePrefix")));
        assertThat(jsonObject, is(object("nested", nestedObject)));
    }

    @Test
    public void shouldCreatePrefixFilter() throws Exception {
        JsonObject jsonObject = QueryBuilders.prefixFilter("someName", "somePrefix").build();
        assertThat(jsonObject, is(object("prefix", object("someName", "somePrefix"))));
    }

    @Test
    public void shouldCreateExistsFilter() throws Exception {
        JsonObject jsonObject = QueryBuilders.existsFilter("someField");
        assertThat(jsonObject, is(object("exists", object("field", "someField"))));
    }

    @Test
    public void shouldCreateAndFilter() {
        JsonObject jsonObject = QueryBuilders.andFilter(new TermQueryBuilder("someName", new JsonPrimitive("someValue"))).build();
        assertThat(jsonObject, is(object("and", array(object("term", object("someName", "someValue"))))));
    }

    @Test
    public void shouldCreateNumberRangeFilter() {
        NumberRangeQueryBuilder rangeQueryBuilder = QueryBuilders.numberRangeFilter("someFieldName").lt(5);
        assertThat(rangeQueryBuilder.build(), is(object("range", object("someFieldName", object("lt", 5)))));
    }

    @Test
    public void shouldCreateDateRangeFilter() throws Exception {
        LocalDate now = LocalDate.now();
        DateRangeQueryBuilder dateRangeQueryBuilder = QueryBuilders.dateRangeFilter("someFieldName").lt(now);
        assertThat(dateRangeQueryBuilder.build(), is(object("range", object("someFieldName", object("lt", now.format(DateTimeFormatter.ISO_DATE))))));
    }
}