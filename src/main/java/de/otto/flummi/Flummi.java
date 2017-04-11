package de.otto.flummi;

import com.google.gson.JsonObject;
import com.ning.http.client.AsyncHttpClient;
import de.otto.flummi.request.*;
import de.otto.flummi.util.HttpClientWrapper;

import java.util.List;
import java.util.Optional;


public class Flummi {
    private final HttpClientWrapper httpClient;

    public Flummi(AsyncHttpClient asyncHttpClient, String baseUrl) {
        this.httpClient = new HttpClientWrapper(asyncHttpClient, baseUrl);
    }

    @Deprecated
    public Optional<String> getIndexNameForAlias(String aliasName) {
        return this.admin().indices().getIndexNameForAlias(aliasName);
    }

    @Deprecated
    public void pointProductAliasToCurrentIndex(String aliasName, String indexName) throws InvalidElasticsearchResponseException {
        admin().indices().pointAliasToCurrentIndex(aliasName, indexName);
    }

    @Deprecated
    public boolean aliasExists(String aliasName) {
        return admin().indices().aliasExists(aliasName);
    }

    @Deprecated
    public List<String> getAllIndexNames() {
        return this.admin().indices().getAllIndexNames();
    }

    @Deprecated
    public JsonObject getIndexSettings() {
        return this.admin().indices().getIndexSettings();
    }

    @Deprecated
    public void refreshIndex(final String indexName) {
        admin().indices().prepareRefresh(indexName).execute();
    }

    public void forceMerge(final String indexName) {
        admin().indices().forceMerge(indexName).execute();
    }

    public SearchRequestBuilder prepareSearch(String... indices) {
        return new SearchRequestBuilder(httpClient, indices);
    }

    public CountRequestBuilder prepareCount(String... indices) {
        return new CountRequestBuilder(httpClient, indices);
    }

    public BulkRequestBuilder prepareBulk() {
        return new BulkRequestBuilder(httpClient);
    }

    public GetRequestBuilder prepareGet(String indexName, String documentType, String id) {
        return new GetRequestBuilder(httpClient, indexName, documentType, id);
    }

    public DeleteRequestBuilder prepareDelete() {
        return new DeleteRequestBuilder(httpClient);
    }

    public MultiGetRequestBuilder prepareMultiGet(String[] indices) {
        return new MultiGetRequestBuilder(httpClient, indices);
    }

    public AnalyzeRequestBuilder prepareAnalyze(String text) {
        return new AnalyzeRequestBuilder(httpClient, text);
    }

    public SearchScrollRequestBuilder prepareScroll() {
        return new SearchScrollRequestBuilder(httpClient);
    }

    // TODO what is the purpose of this builder vs. prepareGet/admin().indices().prepareCreate ?
    public IndexRequestBuilder prepareIndex() {
        return new IndexRequestBuilder(httpClient);
    }

    public AdminClient admin() {
        return new AdminClient(httpClient);
    }
}
