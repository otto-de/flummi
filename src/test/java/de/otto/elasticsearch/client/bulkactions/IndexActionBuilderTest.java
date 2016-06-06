package de.otto.elasticsearch.client.bulkactions;

import de.otto.elasticsearch.client.request.GsonHelper;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class IndexActionBuilderTest {

    @Test
    public void shouldCreateJsonStringWithoutIdForBulkRequest() throws Exception {
        // given
        // when
        String result = new IndexActionBuilder("someIndex")
                .setType("someType")
                .setOpType(IndexOpType.CREATE)
                .setSource(GsonHelper.object("Hello", "World"))
                .toBulkRequestAction();

        // then
        assertThat(result, is("{\"create\":{\"_index\":\"someIndex\",\"_type\":\"someType\"}}\n" +
                "{\"Hello\":\"World\"}"));
    }

    @Test
    public void shouldCreateJsonStringWithIdForBulkRequest() throws Exception {
        // given

        // when
        String result = new IndexActionBuilder("someIndex")
                .setType("someType")
                .setOpType(IndexOpType.INDEX)
                .setSource(GsonHelper.object("Hello", "World"))
                .setId("someId")
                .toBulkRequestAction();

        // then
        assertThat(result, is("{\"index\":{\"_index\":\"someIndex\",\"_type\":\"someType\",\"_id\":\"someId\"}}\n" +
                "{\"Hello\":\"World\"}"));
    }

    @Test
    public void shouldThrowExceptionIfIndexIsEmpty() throws Exception {
        // given

        // when
        try {
            new IndexActionBuilder("")
                    .setType("someType")
                    .setOpType(IndexOpType.INDEX)
                    .setSource(GsonHelper.object("Hello", "World"))
                    .toBulkRequestAction();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("missing property 'index'"));
        }
        // then
    }

    @Test
    public void shouldThrowExceptionIfTypeIsEmpty() throws Exception {
        // given

        // when
        try {
            new IndexActionBuilder("someIndex")
                    .setType("")
                    .setOpType(IndexOpType.INDEX)
                    .setSource(GsonHelper.object("Hello", "World"))
                    .toBulkRequestAction();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("missing property 'type'"));
        }
        // then
    }

    @Test
    public void shouldThrowExceptionIfTypeIsMissing() throws Exception {
        // given

        // when
        try {
            new IndexActionBuilder("someIndex")
                    .setOpType(IndexOpType.INDEX)
                    .setSource(GsonHelper.object("Hello", "World"))
                    .toBulkRequestAction();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("missing property 'type'"));
        }
        // then
    }

    @Test
    public void shouldThrowExceptionIfOpTypeIsMissing() throws Exception {
        // given

        // when
        try {
            new IndexActionBuilder("someIndex")
                    .setType("someType")
                    .setSource(GsonHelper.object("Hello", "World"))
                    .toBulkRequestAction();
        } catch (RuntimeException e) {
            assertThat(e.getMessage(), is("missing property 'opType'"));
        }
        // then
    }
}