package io.tarantool.driver.api.client.parameterwrapper;

public class TarantoolRetryDelayWrapper implements TarantoolClientParameter<Long> {

    private final long delayMs;

    public TarantoolRetryDelayWrapper(long delayMs) {
        this.delayMs = delayMs;
    }

    public TarantoolRetryDelayWrapper() {
        this.delayMs = 0;
    }

    @Override
    public Long getValue() {
        return this.delayMs;
    }
}
