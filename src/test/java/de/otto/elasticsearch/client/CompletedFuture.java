package de.otto.elasticsearch.client;

import com.ning.http.client.ListenableFuture;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class CompletedFuture<T> implements ListenableFuture<T> {
    private T result;

    public CompletedFuture(T result) {
        this.result = result;
    }

    @Override
    public void done() {

    }

    @Override
    public void abort(Throwable t) {

    }

    @Override
    public void touch() {

    }

    @Override
    public ListenableFuture<T> addListener(Runnable listener, Executor exec) {
        return null;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return true;
    }

    @Override
    public T get() throws InterruptedException, ExecutionException {
        return result;
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return result;
    }
}
