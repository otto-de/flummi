package de.otto.flummi.query;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import org.testng.annotations.Test;

import java.util.List;

import static de.otto.flummi.request.GsonHelper.array;
import static de.otto.flummi.request.GsonHelper.object;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class TermsQueryBuilderTest {

    @Test
    public void shouldCreateTermsFromStringList() {
        TermsQueryBuilder termsQueryBuilder = new TermsQueryBuilder("someName", asList("someValue", "someOtherValue"));
        assertThat(termsQueryBuilder.build(), is(object("terms", object("someName", array(
                new JsonPrimitive("someValue"),
                new JsonPrimitive("someOtherValue")
        )))));
    }

    @Test
    public void shouldThrowexceptionIfNameIsMissingForStringList() {
        try {
            new TermsQueryBuilder(null, asList("someValue")).build();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("missing property 'name'"));
        }
    }

    @Test
    public void shouldThrowexceptionIfNameIsEmptyForStringList() {
        try {
            new TermsQueryBuilder("", asList("someValue")).build();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("missing property 'name'"));
        }
    }

    @Test
    public void shouldCreateTermsFromJsonElement() {
        TermsQueryBuilder termsQueryBuilder = new TermsQueryBuilder("someName", new JsonPrimitive("someValue"));
        assertThat(termsQueryBuilder.build(), is(object("terms", object("someName", "someValue"))));
    }

    @Test
    public void shouldThrowexceptionIfNameIsMissingForJsonElement() {
        try {
            new TermsQueryBuilder(null, new JsonPrimitive("someValue")).build();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("missing property 'name'"));
        }
    }

    @Test
    public void shouldThrowexceptionIfNameIsEmptyForElement() {
        try {
            new TermsQueryBuilder("", new JsonPrimitive("someValue")).build();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("missing property 'name'"));
        }
    }

    @Test
    public void shouldThrowExceptionIfListValueIsNull() {
        List<String> testList = null;
        try {
            new TermsQueryBuilder("someName", testList).build();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("missing property 'terms'"));
        }
    }

    @Test
    public void shouldThrowExceptionIfJsonValueIsNull() {
        JsonElement testElement = null;
        try {
            new TermsQueryBuilder("someName", testElement).build();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("missing property 'terms'"));
        }
    }

    @Test
    public void shouldThrowexceptionIfValueIsEmpty() {
        try {
            new TermsQueryBuilder("someName", emptyList()).build();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("missing property 'terms'"));
        }
    }
}