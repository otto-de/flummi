package de.otto.flummi.query;

import com.google.gson.JsonPrimitive;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static de.otto.flummi.request.GsonHelper.array;
import static de.otto.flummi.request.GsonHelper.object;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

@Test
public class BoolQueryBuilderTest {

    private BoolQueryBuilder testee;

    @BeforeMethod
    public void setUp() throws Exception {
        testee = new BoolQueryBuilder();
    }


    @Test
    public void shouldBuildMustBoolQuery() throws Exception {
        // given

        // when
        testee.must(new TermQueryBuilder("someName", new JsonPrimitive("someValue")));

        //then
        assertThat(testee.build(), is(
                object("bool",
                        object("must",
                                object("term",
                                        object("someName", "someValue"))))));
    }

    @Test
    public void shouldBuildMultipleMustBoolQuery() throws Exception {
        // given

        // when
        testee.must(new TermQueryBuilder("someName0", new JsonPrimitive("someValue0")));
        testee.must(new TermQueryBuilder("someName1", new JsonPrimitive("someValue1")).build());

        //then
        assertThat(testee.build(), is(
                object("bool",
                        object("must",
                                array(
                                        object("term",
                                                object("someName0", "someValue0")),
                                        object("term",
                                                object("someName1", "someValue1")))))));
    }

    @Test
    public void shouldBuildMustNotBoolQuery() throws Exception {
        // given

        // when
        testee.mustNot(new TermQueryBuilder("someName", new JsonPrimitive("someValue")).build());

        //then
        assertThat(testee.build(), is(
                object("bool",
                        object("must_not",
                                object("term",
                                        object("someName", "someValue"))))));
    }

    @Test
    public void shouldBuildMultipleMustNotBoolQuery() throws Exception {
        // given

        // when
        testee.mustNot(new TermQueryBuilder("someName0", new JsonPrimitive("someValue0")).build());
        testee.mustNot(new TermQueryBuilder("someName1", new JsonPrimitive("someValue1")).build());

        //then
        assertThat(testee.build(), is(
                object("bool",
                        object("must_not",
                                array(
                                        object("term",
                                                object("someName0", "someValue0")),
                                        object("term",
                                                object("someName1", "someValue1"))
                                )))));
    }




    @Test
    public void shouldBuildShouldBoolQuery() throws Exception {
        // given

        // when
        testee.should(new TermQueryBuilder("someName", new JsonPrimitive("someValue")).build());

        //then
        assertThat(testee.build(), is(
                object("bool",
                        object("should",
                                object("term",
                                        object("someName", "someValue"))))));
    }

    @Test
    public void shouldBuildMultipleShouldBoolQuery() throws Exception {
        // given

        // when
        testee.should(new TermQueryBuilder("someName0", new JsonPrimitive("someValue0")).build());
        testee.should(new TermQueryBuilder("someName1", new JsonPrimitive("someValue1")).build());
        testee.minimumShouldMatch("75%");

        //then
        assertThat(testee.build(), is(
                object("bool",
                        object("should",
                                array(
                                        object("term",
                                                object("someName0", "someValue0")),
                                        object("term",
                                                object("someName1", "someValue1"))
                                ),
                                "minimum_should_match", new JsonPrimitive("75%")))));
    }


    @Test
    public void shouldBuildMultipleMustNotAndMustBoolQuery() throws Exception {
        // given

        // when
        testee.mustNot(new TermQueryBuilder("someName0", new JsonPrimitive("someValue0")).build());
        testee.mustNot(new TermQueryBuilder("someName1", new JsonPrimitive("someValue1")).build());
        testee.must(new TermQueryBuilder("someName2", new JsonPrimitive("someValue2")).build());
        testee.must(new TermQueryBuilder("someName3", new JsonPrimitive("someValue3")).build());

        //then
        assertThat(testee.build(), is(
                object("bool",
                        object(
                                "must_not",
                                array(
                                        object("term",
                                                object("someName0", "someValue0")),
                                        object("term",
                                                object("someName1", "someValue1"))
                                ),
                                "must",
                                array(
                                        object("term",
                                                object("someName2", "someValue2")),
                                        object("term",
                                                object("someName3", "someValue3")))))));
    }

    @Test
    public void shouldThrowExceptionIfMustAndMustNotAreEmpty() throws Exception {
        // given

        // when
        try {
            testee.build();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("mustFilter and mustNotFilter are empty"));
        }

        //then
    }

    @Test
    public void shouldReportToBeEmpty() throws Exception {
        assertThat(testee.isEmpty(), is(true));
    }

}