package io.tarantool.driver.api.space.options;

import java.util.Optional;

/**
 * Marker interface for space replace_many operation options
 *
 * @author Alexey Kuzin
 */
public interface ReplaceManyOptions extends OperationWithTimeoutOptions {
    /**
     * Return whether all changes should not be saved if any tuple replace
     * was unsuccesful.
     *
     * @return true, if the operation should rollback on error
     */
    Optional<Boolean> getRollbackOnError();

    /**
     * Return whether the operation should be interrupted if any tuple replace
     * was unsuccesful.
     *
     * @return true, if the operation should stop on error
     */
    Optional<Boolean> getStopOnError();
}