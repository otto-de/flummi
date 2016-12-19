package de.otto.flummi.extensions;

import de.otto.flummi.IndicesAdminClient;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class RollingIndexBehaviorTest {

    private RollingIndexBehavior behavior;
    private IndicesAdminClient client;

    @BeforeMethod
    public void setUp() throws Exception {
        client = mock(IndicesAdminClient.class, RETURNS_DEEP_STUBS);
        behavior = new RollingIndexBehavior(client, "alias", "prefix", 3);
    }

    @Test
    public void shouldCreateNewIndex() throws Exception {
        // given
        // when
        String result = behavior.createNewIndex();

        // then
        assertTrue(result.startsWith("prefix_"));
    }

    @Test
    public void shouldDeleteIndexOnAbort() throws Exception {
        // given
        String newIndexName = "prefix_744743483";

        // when
        behavior.abort(newIndexName);

        // then
        verify(client).prepareDelete(newIndexName);
        verify(client.prepareDelete(newIndexName)).execute();
    }

    @Test
    public void shouldPointAliasAndDeleteOldIndices() throws Exception {
        // given
        String newIndexName = "prefix_8";

        when(client.getIndexNameForAlias("alias")).thenReturn(Optional.of("prefix_7"));
        when(client.getAllIndexNames()).thenReturn(asList("hurzel", "prefix_1", "prefix_2", "prefix_3", "prefix_4", "prefix_5", "prefix_6", "prefix_7", "prefix_8"));
        // when
        Set<String> result = behavior.commit(newIndexName);

        // then
        verify(client).pointAliasToCurrentIndex("alias", newIndexName);
        verify(client).prepareDelete(any(Stream.class));
        assertEquals(result, asSet("prefix_1", "prefix_2", "prefix_3", "prefix_4", "prefix_5"));
    }

    @Test
    public void shouldPointAliasButNotDeleteOldIndices() throws Exception {
        // given
        String newIndexName = "prefix_8";

        when(client.getIndexNameForAlias("alias")).thenReturn(Optional.of("prefix_7"));
        when(client.getAllIndexNames()).thenReturn(asList());
        // when
        Set<String> result = behavior.commit(newIndexName);

        // then
        verify(client).pointAliasToCurrentIndex("alias", newIndexName);
        verify(client, never()).prepareDelete(any(Stream.class));
        assertEquals(result, asSet());
    }


    private <T> Set<T> asSet(T ... values) {
        return new HashSet<>(Arrays.asList(values));
    }
}