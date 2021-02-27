package io.tarantool.driver;

import io.tarantool.driver.exceptions.TarantoolClientException;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Alexey Kuzin
 */
class RequestRetryPolicyTest {

    private final Executor executor = Executors.newWorkStealingPool();

    @Test
    void testInfiniteRetryPolicy_success() throws ExecutionException, InterruptedException {
        RequestRetryPolicy policy = throwable -> true;
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(this::simpleSuccessFuture, executor);
        assertTrue(wrappedFuture.get());
    }

    @Test
    void testInfiniteRetryPolicy_successWithFuture() throws ExecutionException, InterruptedException {
        RequestRetryPolicy policy = throwable -> true;
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(() -> future, executor);
        future.complete(true);
        assertTrue(wrappedFuture.get());
    }

    @Test
    void testInfiniteRetryPolicy_successAfterFails() throws ExecutionException, InterruptedException {
        RequestRetryPolicy policy = throwable -> true;
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(() -> future, executor);
        future.complete(true);
        assertTrue(wrappedFuture.get());
    }

    @Test
    void testInfiniteRetryPolicy_unretryableError() throws ExecutionException, InterruptedException {
        RequestRetryPolicy policy = throwable -> throwable instanceof TarantoolClientException;
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(this::simpleFailingFuture, executor);
        try {
            wrappedFuture.get();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof RuntimeException);
            assertEquals("Fail", e.getCause().getMessage());
        }
    }

    @Test
    void testAttemptsBoundRetryPolicy_returnSuccessAfterFails() throws ExecutionException, InterruptedException {
        AtomicReference<Integer> retries = new AtomicReference<>(3);
        RequestRetryPolicy policy = TarantoolRequestRetryPolicies.byNumberOfAttempts(3).build().create();
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(
                () -> failingIfAvailableRetriesFuture(retries.getAndUpdate(r -> r - 1)), executor);
        assertTrue(wrappedFuture.get());
    }

    @Test
    void testAttemptsBoundRetryPolicy_returnSuccessAfterFailsWithDelay()
            throws ExecutionException, InterruptedException {
        AtomicReference<Integer> retries = new AtomicReference<>(3);
        RequestRetryPolicy policy = TarantoolRequestRetryPolicies.byNumberOfAttempts(3).withDelay(10).build().create();
        Instant now = Instant.now();
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(
                () -> failingIfAvailableRetriesFuture(retries.getAndUpdate(r -> r - 1)), executor);
        assertTrue(wrappedFuture.get());
        long diff = Instant.now().toEpochMilli() - now.toEpochMilli();
        assertTrue(diff >= 30);
    }

    @Test
    void testAttemptsBoundRetryPolicy_runOutOfAttempts() throws InterruptedException {
        AtomicReference<Integer> retries = new AtomicReference<>(4);
        RequestRetryPolicy policy = TarantoolRequestRetryPolicies.byNumberOfAttempts(3).build().create();
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(
                () -> failingIfAvailableRetriesFuture(retries.getAndUpdate(r -> r - 1)), executor);
        ExecutionException thrown = null;
        try {
            wrappedFuture.get();
        } catch (ExecutionException e) {
            thrown = e;
            assertEquals(RuntimeException.class, e.getCause().getClass());
            assertEquals("Should fail 1 times", e.getCause().getMessage());
        }
        assertNotNull(thrown, "No exception has been thrown");
    }

    @Test
    void testAttemptsBoundRetryPolicy_zeroAttempts() throws ExecutionException, InterruptedException {
        AtomicReference<Integer> retries = new AtomicReference<>(1);
        RequestRetryPolicy policy = TarantoolRequestRetryPolicies.byNumberOfAttempts(0).build().create();
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(
                () -> failingIfAvailableRetriesFuture(retries.getAndUpdate(r -> r - 1)), executor);
        ExecutionException thrown = null;
        try {
            wrappedFuture.get();
        } catch (ExecutionException e) {
            thrown = e;
            assertEquals(RuntimeException.class, e.getCause().getClass());
            assertEquals("Should fail 1 times", e.getCause().getMessage());
        }
        assertNotNull(thrown, "No exception has been thrown");
    }

    @Test
    void testAttemptsBoundRetryPolicy_unretryableError() throws ExecutionException, InterruptedException {
        RequestRetryPolicy policy = TarantoolRequestRetryPolicies.byNumberOfAttempts(4).build().create();
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(this::simpleFailingFuture, executor);
        try {
            wrappedFuture.get();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof RuntimeException);
            assertEquals("Fail", e.getCause().getMessage());
        }
    }

    @Test
    void testAttemptsBoundRetryPolicy_retryTimeouts() throws ExecutionException, InterruptedException {
        RequestRetryPolicy policy = new TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicy<>(
                4, 10, 0, throwable -> throwable instanceof TimeoutException);
        AtomicReference<Integer> retries = new AtomicReference<>(3);
        CompletableFuture<Boolean> wrappedFuture = policy.wrapOperation(
                () -> sleepIfAvailableRetriesFuture(retries.getAndUpdate(r -> r - 1), 20), executor);
        assertTrue(wrappedFuture.get());
    }

    private CompletableFuture<Boolean> simpleSuccessFuture() {
        return CompletableFuture.completedFuture(true);
    }

    private CompletableFuture<Boolean> simpleFailingFuture() {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        result.completeExceptionally(new RuntimeException("Fail"));
        return result;
    }

    private CompletableFuture<Boolean> failingIfAvailableRetriesFuture(int retries) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        if (retries > 0) {
            result.completeExceptionally(new RuntimeException("Should fail " + retries + " times"));
        } else {
            result.complete(true);
        }
        return result;
    }

    private CompletableFuture<Boolean> sleepIfAvailableRetriesFuture(int retries, long timeout) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        if (retries > 0) {
            try {
                Thread.sleep(timeout);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        result.complete(true);
        return result;
    }
}
