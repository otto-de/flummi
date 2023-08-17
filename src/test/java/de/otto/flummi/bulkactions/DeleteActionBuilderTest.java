package de.otto.flummi.bulkactions;

import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class DeleteActionBuilderTest {

    @Test
    public void shouldCreateJsonStringWithoutIdForBulkRequest() throws Exception {
        // given
        // when
        String result = new DeleteActionBuilder("someIndex", "someId", "someType").toBulkRequestAction();

        // then
        assertThat(result, is("{\"delete\":{\"_index\":\"someIndex\",\"_id\":\"someId\",\"_type\":\"someType\"}}"));
    }

    @Test
    public void shouldThrowExceptionIfIdIsEmpty() throws Exception {
        // given
        // when
        try {
            String result = new DeleteActionBuilder("someIndex", "", "someType").toBulkRequestAction();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("missing property 'id'"));
        }
        // then
    }

    @Test
    public void shouldThrowExceptionIfIndexIsEmpty() throws Exception {
        // given
        // when
        try {
            String result = new DeleteActionBuilder("", "someId", "someType").toBulkRequestAction();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("missing property 'index'"));
        }
        // then
    }

    @Test(enabled=false)
    public void shouldThrowExceptionIfTypeIsEmpty() throws Exception {
        // given
        // when
        try {
            String result = new DeleteActionBuilder("someIndex", "someId", "").toBulkRequestAction();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("missing property 'type'"));
        }
        // then
    }

    @Test
    public void shouldSetRouting() throws Exception {
        String result = new DeleteActionBuilder("someIndex", "someId", "someType").setRouting("someRoutingId").toBulkRequestAction();
        assertThat(result, is("{\"delete\":{\"_index\":\"someIndex\",\"_id\":\"someId\",\"_type\":\"someType\",\"_routing\":\"someRoutingId\"}}"));
    }
}