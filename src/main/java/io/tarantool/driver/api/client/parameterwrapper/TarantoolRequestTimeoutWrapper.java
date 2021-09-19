package io.tarantool.driver.api.client.parameterwrapper;

import io.tarantool.driver.api.client.parameterwrapper.TarantoolClientParameter;

public class TarantoolRequestTimeoutWrapper implements TarantoolClientParameter<Long> {

    private final long requestTimeoutMs;

    public TarantoolRequestTimeoutWrapper(long requestTimeoutMs) {
        this.requestTimeoutMs = requestTimeoutMs;
    }

    public TarantoolRequestTimeoutWrapper() {
        this.requestTimeoutMs = 0;
    }

    @Override
    public Long getValue() {
        return this.requestTimeoutMs;
    }
}
