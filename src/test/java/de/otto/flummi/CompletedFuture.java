package de.otto.flummi;

import org.asynchttpclient.ListenableFuture;

import java.util.concurrent.*;

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

	@Override
	public CompletableFuture<T> toCompletableFuture() {
		return null;
	}
}
