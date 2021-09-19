package io.tarantool.driver.api.client.parameterwrapper;

import io.tarantool.driver.api.client.parameterwrapper.TarantoolClientParameter;
import io.tarantool.driver.proxy.ProxyOperationsMappingConfig;

import java.util.function.UnaryOperator;

public class TarantoolProxyOperationsWrapper
        implements TarantoolClientParameter<UnaryOperator<ProxyOperationsMappingConfig.Builder>> {

    private final UnaryOperator<ProxyOperationsMappingConfig.Builder> builder;

    public TarantoolProxyOperationsWrapper(UnaryOperator<ProxyOperationsMappingConfig.Builder> builder) {
        this.builder = builder;
    }

    @Override
    public UnaryOperator<ProxyOperationsMappingConfig.Builder> getValue() {
        return this.builder;
    }
}
