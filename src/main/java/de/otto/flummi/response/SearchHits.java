package de.otto.flummi.response;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

public interface SearchHits extends Iterable<SearchHit> {
    long getTotalHits();

    /**
     * @return The maximum score of this query.
     */
    Float getMaxScore();

    Iterator<SearchHit> iterator();

    void forEach(Consumer<? super SearchHit> action);

    Spliterator<SearchHit> spliterator();

    Stream<SearchHit> stream();
}
