package io.tarantool.driver.api.space.options.contracts;

import io.tarantool.driver.api.space.options.enums.ProxyOption;
import io.tarantool.driver.api.space.options.interfaces.Options;
import io.tarantool.driver.api.space.options.interfaces.Self;

import java.util.Optional;

public interface OperationWIthBatchSizeOptions<T extends OperationWithTimeoutOptions<T>> extends Options, Self<T> {

    /**
     * Specifies internal batch size for transferring data from storage nodes to router nodes.
     *
     * @param batchSize batch size, should be greater than 0
     * @return this options instance
     */
    default T withBatchSize(int batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Batch size should be greater than 0");
        }
        addOption(ProxyOption.BATCH_SIZE, batchSize);
        return self();
    }

    /**
     * Return the internal size of batch for transferring data between
     * storage and router nodes.
     *
     * @return batch size
     */
    default Optional<Integer> getBatchSize() {
        return getOption(ProxyOption.BATCH_SIZE, Integer.class);
    }
}
