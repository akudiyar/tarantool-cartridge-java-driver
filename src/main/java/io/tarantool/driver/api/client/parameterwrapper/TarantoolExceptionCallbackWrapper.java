package io.tarantool.driver.api.client.parameterwrapper;

import io.tarantool.driver.api.client.parameterwrapper.TarantoolClientParameter;

import java.util.function.Function;

public class TarantoolExceptionCallbackWrapper implements TarantoolClientParameter<Function<Throwable, Boolean>> {

    private final Function<Throwable, Boolean> callback;

    public TarantoolExceptionCallbackWrapper(Function<Throwable, Boolean> callback) {
        this.callback = callback;
    }

    public TarantoolExceptionCallbackWrapper() {
        this.callback = (throwable) -> true;
    }

    @Override
    public Function<Throwable, Boolean> getValue() {
        return this.callback;
    }
}
