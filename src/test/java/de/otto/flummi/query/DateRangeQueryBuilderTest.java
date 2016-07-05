package de.otto.flummi.query;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

import static de.otto.flummi.request.GsonHelper.object;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class DateRangeQueryBuilderTest {

    private DateRangeQueryBuilder testee;

    @BeforeMethod
    public void setUp() throws Exception {
        testee = new DateRangeQueryBuilder("someField");
    }

    @Test
    public void shouldCreateLtDateQuery() throws Exception {
        // given

        // when
        LocalDate today = LocalDate.now();
        testee.lt(today);

        //then
        assertThat(testee.build(), is(
                object("range",
                        object("someField",
                                object("lt", today.format(DateTimeFormatter.ISO_DATE))))
        ));
    }

    @Test
    public void shouldCreateLteDateQuery() throws Exception {
        // given

        // when
        LocalDate today = LocalDate.now();
        testee.lte(today);

        //then
        assertThat(testee.build(), is(
                object("range",
                        object("someField",
                                object("lte", today.format(DateTimeFormatter.ISO_DATE))))
        ));
    }

    @Test
    public void shouldCreateGtDateQuery() throws Exception {
        // given

        // when
        LocalDate today = LocalDate.now();
        testee.gt(today);

        //then
        assertThat(testee.build(), is(
                object("range",
                        object("someField",
                                object("gt", today.format(DateTimeFormatter.ISO_DATE))))
        ));
    }

    @Test
    public void shouldCreateGteDateQuery() throws Exception {
        // given

        // when
        LocalDate today = LocalDate.now();
        testee.gte(today);

        //then
        assertThat(testee.build(), is(
                object("range",
                        object("someField",
                                object("gte", today.format(DateTimeFormatter.ISO_DATE))))
        ));
    }

    @Test
    public void shouldCreateLtAndGteDateQuery() throws Exception {
        // given

        // when
        LocalDate today = LocalDate.now();
        testee
                .gte(today.minus(3, ChronoUnit.DAYS))
                .lt(today.plus(1, ChronoUnit.DAYS));

        //then
        assertThat(testee.build(), is(
                object("range",
                        object("someField",
                                object("gte", today.minus(3, ChronoUnit.DAYS).format(DateTimeFormatter.ISO_DATE),
                                        "lt", today.plus(1, ChronoUnit.DAYS).format(DateTimeFormatter.ISO_DATE))))));
    }

    @Test
    public void shouldThrowExceptionIfFieldNameIsEmpty() throws Exception {
        // given

        // when
        try {
            new DateRangeQueryBuilder("").lt(LocalDate.now()).build();
        }catch (RuntimeException e) {
            assertThat(e.getMessage(), is("fieldName is missing"));
        }
        //then
    }

    @Test
    public void shouldThrowExceptionIfFromAndToAreMissing() throws Exception {
        // given

        // when
        try {
            new DateRangeQueryBuilder("someFieldName").build();
        }catch (RuntimeException e) {
            assertThat(e.getMessage(), is("from and to fields are missing"));
        }
        //then
    }
}