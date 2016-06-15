package de.otto.elasticsearch.client.response;


import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Simple implementation of SearchHits. Only contains a single page of search results
 * and does not do any automatic fetching of more pages.
 */
public class SimpleSearchHits implements SearchHits {
    private final long totalHits;
    private final Float maxScore;
    private final List<SearchHit> hits;

    public SimpleSearchHits(long totalHits, Float maxScore, List<SearchHit> hits) {
        this.totalHits = totalHits;
        this.maxScore = maxScore;
        this.hits = hits;
    }

    @Override
    public long getTotalHits(){
        return totalHits;
    };

    @Override
    public Float getMaxScore(){
        return maxScore;
    };

    /**
     * The hits of the search request (based on the search type, and from / size provided).
     */
    public List<SearchHit> getHits(){
        return hits;
    };

    @Override
    public Iterator<SearchHit> iterator() {
        return hits.iterator();
    }

    @Override
    public void forEach(Consumer<? super SearchHit> action) {
        hits.forEach(action);
    }

    @Override
    public Spliterator<SearchHit> spliterator() {
        return hits.spliterator();
    }

    @Override
    public Stream<SearchHit> stream() {
        return hits.stream();
    }
}

