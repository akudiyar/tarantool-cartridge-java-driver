package io.tarantool.driver.api.client.parameterwrapper;

import io.tarantool.driver.api.client.parameterwrapper.TarantoolClientParameter;

public class TarantoolRetryAttemptsWrapper implements TarantoolClientParameter<Integer> {

    private final int numberOfAttempts;

    public TarantoolRetryAttemptsWrapper(int numberOfAttempts) {
        this.numberOfAttempts = numberOfAttempts;
    }

    @Override
    public Integer getValue() {
        return this.numberOfAttempts;
    }
}
