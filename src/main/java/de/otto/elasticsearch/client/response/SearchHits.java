package de.otto.elasticsearch.client.response;


import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class SearchHits implements Iterable<SearchHit> {
private long totalHits;
    private Float maxScore;
    private List<SearchHit> hits;

    public SearchHits(long totalHits, Float maxScore, List<SearchHit> hits) {
        this.totalHits = totalHits;
        this.maxScore = maxScore;
        this.hits = hits;
    }

    /**
     * The total number of hits that matches the search request.
     */
    public long getTotalHits(){
        return totalHits;
    };

    /**
     * The maximum score of this query.
     */
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

    public Stream<SearchHit> stream() {
        return hits.stream();
    }
}

