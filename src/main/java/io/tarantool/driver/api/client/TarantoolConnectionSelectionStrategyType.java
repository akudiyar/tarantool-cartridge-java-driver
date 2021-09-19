package io.tarantool.driver.api.client;

import io.tarantool.driver.ConnectionSelectionStrategyFactory;
import io.tarantool.driver.core.TarantoolConnectionSelectionStrategies.ParallelRoundRobinStrategyFactory;
import io.tarantool.driver.core.TarantoolConnectionSelectionStrategies.RoundRobinStrategyFactory;

public enum TarantoolConnectionSelectionStrategyType {

    ROUND_ROBIN(RoundRobinStrategyFactory.INSTANCE),
    PARALLEL_ROUND_ROBIN(ParallelRoundRobinStrategyFactory.INSTANCE);

    private final ConnectionSelectionStrategyFactory value;

    TarantoolConnectionSelectionStrategyType(ConnectionSelectionStrategyFactory value) {
        this.value = value;
    }

    public ConnectionSelectionStrategyFactory value() {
        return value;
    }

    static TarantoolConnectionSelectionStrategyType defaultType() {
        return TarantoolConnectionSelectionStrategyType.ROUND_ROBIN;
    }
}