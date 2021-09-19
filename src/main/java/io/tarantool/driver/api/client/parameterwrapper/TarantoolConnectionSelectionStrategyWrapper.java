package io.tarantool.driver.api.client.parameterwrapper;

import io.tarantool.driver.api.client.TarantoolConnectionSelectionStrategyType;
import io.tarantool.driver.api.client.parameterwrapper.TarantoolClientParameter;

public class TarantoolConnectionSelectionStrategyWrapper implements TarantoolClientParameter<TarantoolConnectionSelectionStrategyType> {

    private final TarantoolConnectionSelectionStrategyType tarantoolConnectionSelectionStrategyType;

    public TarantoolConnectionSelectionStrategyWrapper(TarantoolConnectionSelectionStrategyType tarantoolConnectionSelectionStrategyType) {
        this.tarantoolConnectionSelectionStrategyType = tarantoolConnectionSelectionStrategyType;
    }

    @Override
    public TarantoolConnectionSelectionStrategyType getValue() {
        return this.tarantoolConnectionSelectionStrategyType;
    }
}
