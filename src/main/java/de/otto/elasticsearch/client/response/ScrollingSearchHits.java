package de.otto.elasticsearch.client.response;

import de.otto.elasticsearch.client.ElasticSearchHttpClient;
import de.otto.elasticsearch.client.request.SearchScrollRequestBuilder;
import de.otto.elasticsearch.client.util.RoundRobinLoadBalancingHttpClient;
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
    private final RoundRobinLoadBalancingHttpClient client;
    private List<SearchHit> hitsCurrentPage;
    private boolean dirty;
    public static final Logger LOG = getLogger(ScrollingSearchHits.class);


    public ScrollingSearchHits(long totalHits, Float maxScore, String scrollId, String scroll, List<SearchHit> hitsCurrentPage, RoundRobinLoadBalancingHttpClient client) {
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

    private boolean hasNextElement(int index) {
        if (index < hitsCurrentPage.size()) {
            return true;
        }
        if(index==0 || this.hitsCurrentPage.isEmpty()) {
            return false;
        }
        fetchNextPage();
        index = 0;
        return index < hitsCurrentPage.size();
    }

    @Override
    public Iterator<SearchHit> iterator() {
        assertNotDirty();
        return new Iterator<SearchHit>() {
            int currentPageIdx = 0;

            @Override
            public boolean hasNext() {
                return hasNextElement(currentPageIdx);
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
            int index = 0;
            @Override
            public boolean tryAdvance(Consumer<? super SearchHit> action) {
                if(!hasNextElement(index)) {
                    return false;
                }
                if (index >= hitsCurrentPage.size()) {
                    LOG.error("index of " + index + " is  >= than current page size of " + hitsCurrentPage.size());
                    return false;
                }
                action.accept(hitsCurrentPage.get(index++));
                return true;
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
