package de.otto.elasticsearch.client.response;

import de.otto.elasticsearch.client.request.SearchScrollRequestBuilder;
import de.otto.elasticsearch.client.util.HttpClientWrapper;
import org.slf4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Auto-scrolling implementation of SearchHits. Contains a page of search results
 * and automatically fetches more pages from the server as you iterate or stream over the search result.
 */
public class ScrollingSearchHits implements SearchHits {
    private final long totalHits;
    private final Float maxScore;
    private final String scrollId;
    private final String scroll;
    private final HttpClientWrapper client;
    private List<SearchHit> hitsCurrentPage;
    private boolean dirty;
    public static final Logger LOG = getLogger(ScrollingSearchHits.class);


    public ScrollingSearchHits(long totalHits, Float maxScore, String scrollId, String scroll, List<SearchHit> hitsCurrentPage, HttpClientWrapper client) {
        this.totalHits = totalHits;
        this.maxScore = maxScore;
        this.scrollId = scrollId;
        this.scroll = scroll;
        this.hitsCurrentPage = hitsCurrentPage;
        this.client = client;
    }


    @Override
    public long getTotalHits() {
        return totalHits;
    }

    @Override
    public Float getMaxScore() {
        return maxScore;
    }

    @Override
    public Iterator<SearchHit> iterator() {
        assertNotDirty();
        return new Iterator<SearchHit>() {
            int currentPageIdx = 0;

            @Override
            public boolean hasNext() {
                if (currentPageIdx < hitsCurrentPage.size()) {
                    return true;
                }
                if(hitsCurrentPage.isEmpty()) {
                    return false;
                }
                fetchNextPage();
                currentPageIdx = 0;
                return !hitsCurrentPage.isEmpty();
            }

            @Override
            public SearchHit next() {
                if(currentPageIdx>0 && currentPageIdx==hitsCurrentPage.size()) {
                    fetchNextPage();
                    currentPageIdx = 0;
                }
                return hitsCurrentPage.get(currentPageIdx++);
            }
        };
    }

    private void assertNotDirty() {
        if(dirty) {
            throw new IllegalStateException("Result was already iterated / streamed before");
        }
    }

    private void fetchNextPage() {
        dirty = true;
        SearchResponse response = new SearchScrollRequestBuilder(client)
                .setScroll(scroll)
                .setScrollId(scrollId)
                .execute();
        this.hitsCurrentPage = ((SimpleSearchHits)response.getHits()).getHits();
    }

    @Override
    public void forEach(Consumer<? super SearchHit> action) {
        assertNotDirty();
        while(!hitsCurrentPage.isEmpty()) {
            hitsCurrentPage.forEach(action);
            fetchNextPage();
        }
    }

    @Override
    public Spliterator<SearchHit> spliterator() {
        assertNotDirty();
        return new Spliterator<SearchHit>() {
            Iterator<SearchHit> iterator = iterator();
            @Override
            public boolean tryAdvance(Consumer<? super SearchHit> action) {
                if(iterator.hasNext()) {
                    action.accept(iterator.next());
                    return true;
                }
                return false;
            }

            @Override
            public Spliterator<SearchHit> trySplit() {
                return null;
            }

            @Override
            public long estimateSize() {
                return totalHits;
            }

            @Override
            public int characteristics() {
                return ORDERED | SIZED | NONNULL | IMMUTABLE;
            }
        };
    }

    @Override
    public Stream<SearchHit> stream() {
        return StreamSupport.stream(spliterator(), false);
    }
}
