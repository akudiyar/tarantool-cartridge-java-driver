package io.tarantool.driver.api.client;

import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.proxy.ProxyOperationsMappingConfig;

import java.util.function.Function;
import java.util.function.UnaryOperator;

public interface TarantoolClientBuilder {

    TarantoolClientBuilder withAddress(TarantoolServerAddress... address);

    TarantoolClientBuilder withCredentials(TarantoolCredentials tarantoolCredentials);

    TarantoolClientBuilder withConnectionSelectionStrategy(
            TarantoolConnectionSelectionStrategyType tarantoolConnectionSelectionStrategyType);

    TarantoolClientBuilder withProxyMapping(UnaryOperator<ProxyOperationsMappingConfig.Builder> builder);

    TarantoolClientBuilder withRetryAttemptsInAmount(int numberOfAttempts);

    TarantoolClientBuilder withRetryDelay(long delayMs);

    TarantoolClientBuilder withRequestTimeout(long requestTimeoutMs);

    TarantoolClientBuilder withExceptionCallback(Function<Throwable, Boolean> callback);


    TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> build();
}
