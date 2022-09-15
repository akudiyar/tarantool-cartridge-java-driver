package io.tarantool.driver.api.space;

import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.cursor.TarantoolCursor;
import io.tarantool.driver.api.metadata.TarantoolSpaceMetadata;
import io.tarantool.driver.api.tuple.operations.TupleOperations;
import io.tarantool.driver.core.space.options.Options;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.protocol.Packable;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Tarantool space operations interface (create, insert, replace, delete...)
 *
 * @param <T> tuple type
 * @param <R> tuple collection type
 * @author Alexey Kuzin
 */
public interface TarantoolSpaceOperations<T extends Packable, R extends Collection<T>> {

    /**
     * Delete a tuple. Only a single primary index value condition is supported.
     *
     * @param conditions query with options
     * @return a future that will contain removed tuple once completed
     * @throws TarantoolClientException in case if the request failed
     */
    CompletableFuture<R> delete(Conditions conditions) throws TarantoolClientException;

    /**
     * Delete a tuple. Only a single primary index value condition is supported.
     *
     * @param conditions query with options
     * @param options specified options
     * @return a future that will contain removed tuple once completed
     * @throws TarantoolClientException in case if the request failed
     */
     default CompletableFuture<R> delete(Conditions conditions, Options options) throws TarantoolClientException {
         return delete(conditions);
     }

    /**
     * Inserts tuple into the space, if no tuple with same unique keys exists. Otherwise throw duplicate key error.
     *
     * @param tuple new data
     * @return a future that will contain all corresponding tuples once completed
     * @throws TarantoolClientException in case if request failed
     */
    CompletableFuture<R> insert(T tuple) throws TarantoolClientException;

    /**
     * Inserts tuple into the space, if no tuple with same unique keys exists. Otherwise throw duplicate key error.
     *
     * @param tuple new data
     * @param options specified options
     * @return a future that will contain all corresponding tuples once completed
     * @throws TarantoolClientException in case if request failed
     */
    default CompletableFuture<R> insert(T tuple, Options options) throws TarantoolClientException {
        return insert(tuple);
    };

    /**
     * Insert a tuple into the space or replace an existing one.
     *
     * @param tuple new data
     * @return a future that will contain all corresponding tuples once completed
     * @throws TarantoolClientException in case if request failed
     */
    CompletableFuture<R> replace(T tuple) throws TarantoolClientException;

    /**
     * Insert a tuple into the space or replace an existing one.
     *
     * @param tuple new data
     * @param options specified options
     * @return a future that will contain all corresponding tuples once completed
     * @throws TarantoolClientException in case if request failed
     */
    default CompletableFuture<R> replace(T tuple, Options options) throws TarantoolClientException {
        return replace(tuple);
    };

    /**
     * Select tuples matching the specified query with specified conditions.
     *
     * @param conditions query with options
     * @return a future that will contain all corresponding tuples once completed
     * @throws TarantoolClientException in case if the request failed
     */
    CompletableFuture<R> select(Conditions conditions) throws TarantoolClientException;

    /**
     * Select tuples matching the specified query with specified conditions and options.
     *
     * @param conditions specified conditions
     * @param options specified options
     * @return a future that will contain all corresponding tuples once completed
     * @throws TarantoolClientException in case if the request failed
     */
    default CompletableFuture<R> select(Conditions conditions, Options options) throws TarantoolClientException {
        return select(conditions);
    };

    /**
     * Update a tuple. Only a single primary index value condition is supported.
     *
     * @param conditions query with options
     * @param tuple tuple with new field values
     * @return a future that will contain corresponding tuple once completed
     * @throws TarantoolClientException in case if the request failed
     */
    CompletableFuture<R> update(Conditions conditions, T tuple);

    /**
     * Update a tuple. Only a single primary index value condition is supported.
     *
     * @param conditions query with options
     * @param tuple tuple with new field values
     * @param options specified options
     * @return a future that will contain corresponding tuple once completed
     * @throws TarantoolClientException in case if the request failed
     */
    default CompletableFuture<R> update(Conditions conditions, T tuple, Options options) {
        return update(conditions, tuple);
    };

    /**
     * Update a tuple. Only a single primary index value condition is supported.
     *
     * @param conditions query with options
     * @param operations the list update operations
     * @return a future that will contain corresponding tuple once completed
     * @throws TarantoolClientException in case if the request failed
     */
    CompletableFuture<R> update(Conditions conditions, TupleOperations operations);

    /**
     * Update a tuple. Only a single primary index value condition is supported.
     *
     * @param conditions query with options
     * @param operations the list update operations
     * @param options specified options
     * @return a future that will contain corresponding tuple once completed
     * @throws TarantoolClientException in case if the request failed
     */
    default CompletableFuture<R> update(Conditions conditions, TupleOperations operations, Options options) {
        return update(conditions, operations);
    };

    /**
     * Update tuple if it would be found elsewhere try to insert tuple. Only a single primary index value condition
     * is supported.
     *
     * @param conditions query with options
     * @param tuple new data that will be insert if tuple will be not found
     * @param operations the list of update operations to be performed if the tuple exists
     * @return a future that will empty list
     * @throws TarantoolClientException in case if the request failed
     */
    CompletableFuture<R> upsert(Conditions conditions, T tuple, TupleOperations operations);

    /**
     * Update tuple if it would be found elsewhere try to insert tuple. Only a single primary index value condition
     * is supported.
     *
     * @param conditions query with options
     * @param tuple new data that will be insert if tuple will be not found
     * @param operations the list of update operations to be performed if the tuple exists
     * @param options specified options
     * @return a future that will empty list
     * @throws TarantoolClientException in case if the request failed
     */
    default CompletableFuture<R> upsert(Conditions conditions, T tuple, TupleOperations operations, Options options) {
        return upsert(conditions, tuple, operations);
    };

    /**
     * Truncate space if space would be found. Otherwise, throw space not found error.
     *
     * @return a future that will contain void.
     * @throws TarantoolClientException in case if request failed
     */
    CompletableFuture<Void> truncate() throws TarantoolClientException;

    /**
     * Get metadata associated with this space
     *
     * @return space metadata
     */
    TarantoolSpaceMetadata getMetadata();

    /**
     * Cursor is an iterator-like object that is able to scroll through
     * results of a query. Unlike a single cursor loads new tuples
     * dynamically issuing requests to server.
     *
     * Select will fetch tuples matching the specified query.
     * Each request to server will fetch no more than 'batch size' tuples.
     *
     * @param conditions query with options
     * @param batchSize  size of a batch of single client request
     * @return cursor that can iterate through all corresponding tuples
     */
    TarantoolCursor<T> cursor(Conditions conditions, int batchSize);

    /**
     * Same as {@link TarantoolSpaceOperations#cursor(Conditions, int)}
     * but uses the default batch size.
     *
     * @param conditions query with options
     * @return cursor that can iterate through all corresponding tuples
     */
    TarantoolCursor<T> cursor(Conditions conditions);
}
