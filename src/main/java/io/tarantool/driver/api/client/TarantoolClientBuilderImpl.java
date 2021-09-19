package io.tarantool.driver.api.client;

import io.tarantool.driver.ClusterTarantoolTupleClient;
import io.tarantool.driver.ProxyTarantoolTupleClient;
import io.tarantool.driver.TarantoolClientConfig;
import io.tarantool.driver.TarantoolServerAddress;
import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.client.parameterwrapper.TarantoolClientParameter;
import io.tarantool.driver.api.client.parameterwrapper.TarantoolConnectionSelectionStrategyWrapper;
import io.tarantool.driver.api.client.parameterwrapper.TarantoolCredentialsWrapper;
import io.tarantool.driver.api.client.parameterwrapper.TarantoolExceptionCallbackWrapper;
import io.tarantool.driver.api.client.parameterwrapper.TarantoolProxyOperationsWrapper;
import io.tarantool.driver.api.client.parameterwrapper.TarantoolRequestTimeoutWrapper;
import io.tarantool.driver.api.client.parameterwrapper.TarantoolRetryAttemptsWrapper;
import io.tarantool.driver.api.client.parameterwrapper.TarantoolRetryDelayWrapper;
import io.tarantool.driver.api.client.parameterwrapper.TarantoolServerAddressWrapper;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.auth.TarantoolCredentials;
import io.tarantool.driver.proxy.ProxyOperationsMappingConfig;
import io.tarantool.driver.retry.RequestRetryPolicyFactory;
import io.tarantool.driver.retry.RetryingTarantoolTupleClient;
import io.tarantool.driver.retry.TarantoolRequestRetryPolicies;
import io.tarantool.driver.retry.TarantoolRequestRetryPolicies.InfiniteRetryPolicyFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.UnaryOperator;

public class TarantoolClientBuilderImpl implements TarantoolClientBuilder {


    private final Map<ParameterType, TarantoolClientParameter<?>> parameters;

    public TarantoolClientBuilderImpl() {
        this.parameters = new HashMap<>();
    }

    @Override
    public TarantoolClientBuilder withAddress(TarantoolServerAddress... address) {
        parameters.put(ParameterType.ADDRESS, new TarantoolServerAddressWrapper(Arrays.asList(address)));
        return this;
    }

    @Override
    public TarantoolClientBuilder withCredentials(TarantoolCredentials credentials) {
        parameters.put(ParameterType.CREDENTIALS, new TarantoolCredentialsWrapper(credentials));
        return this;
    }

    @Override
    public TarantoolClientBuilder withConnectionSelectionStrategy(
            TarantoolConnectionSelectionStrategyType tarantoolConnectionSelectionStrategyType) {
        parameters.put(ParameterType.CONNECTION_SELECTION_STRATEGY,
                new TarantoolConnectionSelectionStrategyWrapper(tarantoolConnectionSelectionStrategyType));
        return this;
    }

    @Override
    public TarantoolClientBuilder withProxyMapping(UnaryOperator<ProxyOperationsMappingConfig.Builder> builder) {
        parameters.put(ParameterType.PROXY_MAPPING, new TarantoolProxyOperationsWrapper(builder));
        return this;
    }

    @Override
    public TarantoolClientBuilder withRetryAttemptsInAmount(int numberOfAttempts) {
        parameters.put(ParameterType.RETRY_ATTEMPTS, new TarantoolRetryAttemptsWrapper(numberOfAttempts));
        return this;
    }

    @Override
    public TarantoolClientBuilder withRetryDelay(long delayMs) {
        parameters.put(ParameterType.RETRY_DELAY, new TarantoolRetryDelayWrapper(delayMs));
        return this;
    }

    @Override
    public TarantoolClientBuilder withRequestTimeout(long requestTimeoutMs) {
        parameters.put(ParameterType.REQUEST_TIMEOUT, new TarantoolRequestTimeoutWrapper(requestTimeoutMs));
        return this;
    }

    @Override
    public TarantoolClientBuilder withExceptionCallback(Function<Throwable, Boolean> callback) {
        parameters.put(ParameterType.EXCEPTION_CALLBACK, new TarantoolExceptionCallbackWrapper(callback));
        return this;
    }

    @Override
    public TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> build() {
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = makeBaseClient();

        if (isParametersContainsProxySettings()) {
            client = makeProxyClient(client);
        }

        if (isParametersContainsRetrySettings()) {
            client = makeRetryingClient(client);
        }

        return client;
    }

    private RetryingTarantoolTupleClient makeRetryingClient(
            TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client) {
        return new RetryingTarantoolTupleClient(client, makeRetryPolicyFactory());
    }

    private boolean isParametersContainsProxySettings() {
        return this.parameters.containsKey(ParameterType.PROXY_MAPPING);
    }

    private boolean isParametersContainsRetrySettings() {
        return this.parameters.containsKey(ParameterType.RETRY_DELAY)
                || this.parameters.containsKey(ParameterType.REQUEST_TIMEOUT)
                || this.parameters.containsKey(ParameterType.RETRY_ATTEMPTS);
    }

    private ProxyTarantoolTupleClient makeProxyClient(TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client) {
        UnaryOperator<ProxyOperationsMappingConfig.Builder> value =
                ((TarantoolProxyOperationsWrapper) this.parameters.get(ParameterType.PROXY_MAPPING)).getValue();
        ProxyOperationsMappingConfig config = value.apply(ProxyOperationsMappingConfig.builder()).build();

        return new ProxyTarantoolTupleClient(client, config);
    }

    private RequestRetryPolicyFactory makeRetryPolicyFactory() {
        Function<Throwable, Boolean> callback = getCallback();
        Integer numberOfAttempts = getNumberOfAttempts();
        Long requestTimeout = getRequestTimeout();
        Long delayMs = getDelay();

        if (numberOfAttempts > 0) {
            return TarantoolRequestRetryPolicies.AttemptsBoundRetryPolicyFactory
                    .builder(numberOfAttempts, callback)
                    .withRequestTimeout(requestTimeout)
                    .withDelay(delayMs)
                    .build();
        }

        return InfiniteRetryPolicyFactory
                .builder(callback)
                .withDelay(delayMs)
                .withRequestTimeout(requestTimeout)
                .build();
    }

    private Integer getNumberOfAttempts() {
        return ((TarantoolRetryAttemptsWrapper)
                this.parameters.getOrDefault(ParameterType.RETRY_ATTEMPTS,
                        new TarantoolRetryAttemptsWrapper(0))).getValue();
    }

    private Long getDelay() {
        return ((TarantoolRetryDelayWrapper) this.parameters.getOrDefault(ParameterType.RETRY_DELAY,
                new TarantoolRetryDelayWrapper())).getValue();
    }

    private Long getRequestTimeout() {
        return ((TarantoolRequestTimeoutWrapper) this.parameters.getOrDefault(
                ParameterType.REQUEST_TIMEOUT, new TarantoolRequestTimeoutWrapper())).getValue();
    }

    private Function<Throwable, Boolean> getCallback() {
        return ((TarantoolExceptionCallbackWrapper) this.parameters.getOrDefault(
                ParameterType.EXCEPTION_CALLBACK, new TarantoolExceptionCallbackWrapper())).getValue();
    }

    private ClusterTarantoolTupleClient makeBaseClient() {
        List<TarantoolServerAddress> addressList = getAddressList();
        TarantoolCredentials credentials = getCredentials();
        TarantoolClientConfig clientConfig = getConfig(credentials);

        return new ClusterTarantoolTupleClient(clientConfig, addressList);
    }

    private TarantoolCredentials getCredentials() {
        return ((TarantoolCredentialsWrapper)
                this.parameters.getOrDefault(ParameterType.CREDENTIALS,
                        new TarantoolCredentialsWrapper(new SimpleTarantoolCredentials())
                )).getValue();
    }

    private List<TarantoolServerAddress> getAddressList() {
        return ((TarantoolServerAddressWrapper)
                this.parameters.getOrDefault(ParameterType.ADDRESS,
                        new TarantoolServerAddressWrapper(Collections.singletonList(new TarantoolServerAddress()))
                )).getValue();
    }

    private TarantoolClientConfig getConfig(TarantoolCredentials credentials) {
        TarantoolConnectionSelectionStrategyType selectionStrategy =
                ((TarantoolConnectionSelectionStrategyWrapper)
                        this.parameters.getOrDefault(ParameterType.CONNECTION_SELECTION_STRATEGY,
                                new TarantoolConnectionSelectionStrategyWrapper(
                                        TarantoolConnectionSelectionStrategyType.defaultType())
                        )).getValue();

        return TarantoolClientConfig.builder()
                .withConnectionSelectionStrategyFactory(selectionStrategy.value())
                .withCredentials(credentials)
                .build();
    }

}
