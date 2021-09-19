package io.tarantool.driver.api.client;

import io.tarantool.driver.ClusterTarantoolTupleClient;
import io.tarantool.driver.ProxyTarantoolTupleClient;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.retry.RetryingTarantoolTupleClient;
import org.junit.jupiter.api.Test;

import static io.tarantool.driver.api.client.TarantoolConnectionSelectionStrategyType.PARALLEL_ROUND_ROBIN;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TarantoolClientBuilderTest {

    @Test
    void test_should_createClient() {
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = TarantoolClientFactory.createClient()
                .withAddress(new TarantoolServerAddress("123.123.123.123", 3333))
                .withCredentials(new SimpleTarantoolCredentials("root", "passwd"))
                .build();

        assertEquals(ClusterTarantoolTupleClient.class, client.getClass());
    }

    @Test
    void test_should_createProxyClient() {
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = TarantoolClientFactory.createClient()
                .withAddress(new TarantoolServerAddress("123.123.123.123", 3333))
                .withCredentials(new SimpleTarantoolCredentials("root", "passwd"))
                .withProxyMapping(builder -> builder.withDeleteFunctionName("createTest"))
                .build();

        assertEquals(ProxyTarantoolTupleClient.class, client.getClass());
    }

    @Test
    void test_should_createRetryingClient() {
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = TarantoolClientFactory.createClient()
                .withAddress(new TarantoolServerAddress("123.123.123.123", 3333))
                .withCredentials(new SimpleTarantoolCredentials("root", "passwd"))
                .withRetryAttemptsInAmount(5)
                .build();

        assertEquals(RetryingTarantoolTupleClient.class, client.getClass());
    }

    @Test
    void test_should_createRetryingWithMoreSettingsClient() {
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = TarantoolClientFactory.createClient()
                .withAddress(new TarantoolServerAddress("123.123.123.123", 3333))
                .withCredentials(new SimpleTarantoolCredentials("root", "passwd"))
                .withRetryAttemptsInAmount(5)
                .withRetryDelay(500)
                .withConnectionSelectionStrategy(PARALLEL_ROUND_ROBIN)
                .build();

        assertEquals(RetryingTarantoolTupleClient.class, client.getClass());
    }

    @Test
    void test_should_createRetryingWithInfiniteAttemptsClient() {
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = TarantoolClientFactory.createClient()
                .withAddress(new TarantoolServerAddress("123.123.123.123", 3333))
                .withCredentials(new SimpleTarantoolCredentials("root", "passwd"))
                .withRetryDelay(500)
                .withRequestTimeout(4444)
                .withConnectionSelectionStrategy(PARALLEL_ROUND_ROBIN)
                .build();

        assertEquals(RetryingTarantoolTupleClient.class, client.getClass());
    }

    @Test
    void test_should_createProxyRetryingClient() {
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = TarantoolClientFactory.createClient()
                .withAddress(new TarantoolServerAddress("123.123.123.123", 3333))
                .withCredentials(new SimpleTarantoolCredentials("root", "passwd"))
                .withRetryAttemptsInAmount(5)
                .withRetryDelay(500)
                .withConnectionSelectionStrategy(PARALLEL_ROUND_ROBIN)
                .withProxyMapping(builder -> builder.withReplaceFunctionName("hello")
                        .withTruncateFunctionName("create"))
                .withExceptionCallback(throwable -> throwable.getMessage().equals("Hello World"))
                .withRequestTimeout(123)
                .build();

        assertEquals(RetryingTarantoolTupleClient.class, client.getClass());
    }
}
