package io.tarantool.driver.mappers;

import io.tarantool.driver.api.TarantoolResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Manages instantiation of the operation result factories
 *
 * @author Alexey Kuzin
 */
public final class DefaultResultMapperFactoryFactory implements ResultMapperFactoryFactory {

    private final TarantoolTupleResultMapperFactory defaultTupleResultFactory;
    private final TarantoolTupleSingleResultMapperFactory defaultTupleSingleResultFactory;
    private final TarantoolTupleMultiResultMapperFactory defaultTupleMultiResultFactory;

    private static final Map<Class<?>, TupleResultMapperFactory<?>> tupleResultFactoryCache = new HashMap<>();
    private static final Map<Class<?>, SingleValueResultMapperFactory<?>> singleResultFactoryCache = new HashMap<>();
    private static final Map<Class<?>, SingleValueTarantoolResultMapperFactory<?>> singleTarantoolResultFactoryCache
            = new HashMap<>();
    private static final Map<Class<?>, MultiValueResultMapperFactory<?, ?>> multiResultFactoryCache = new HashMap<>();
    private static final Map<Class<?>, MultiValueTarantoolResultMapperFactory<?>> multiTarantoolResultFactoryCache
            = new HashMap<>();

    /**
     * Basic constructor.
     */
    public DefaultResultMapperFactoryFactory() {
        this.defaultTupleResultFactory = new TarantoolTupleResultMapperFactory();
        this.defaultTupleSingleResultFactory = new TarantoolTupleSingleResultMapperFactory();
        this.defaultTupleMultiResultFactory = new TarantoolTupleMultiResultMapperFactory();
    }

    @Override
    public TarantoolTupleResultMapperFactory defaultTupleResultMapperFactory() {
        return defaultTupleResultFactory;
    }

    @Override
    public TarantoolTupleSingleResultMapperFactory defaultTupleSingleResultMapperFactory() {
        return defaultTupleSingleResultFactory;
    }

    @Override
    public TarantoolTupleMultiResultMapperFactory defaultTupleMultiResultMapperFactory() {
        return defaultTupleMultiResultFactory;
    }

    @Override
    public <T> TupleResultMapperFactory<T> tupleResultMapperFactory() {
        return new TupleResultMapperFactory<>();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> TupleResultMapperFactory<T> tupleResultMapperFactory(Class<T> tupleClass) {
        return (TupleResultMapperFactory<T>) tupleResultFactoryCache
                .computeIfAbsent(tupleClass, cls -> new TupleResultMapperFactory<>());
    }

    @Override
    public <T> SingleValueResultMapperFactory<T> singleValueResultMapperFactory() {
        return new SingleValueResultMapperFactory<>();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> SingleValueResultMapperFactory<T> singleValueResultMapperFactory(Class<T> resultClass) {
        return (SingleValueResultMapperFactory<T>) singleResultFactoryCache
                .computeIfAbsent(resultClass, cls -> new SingleValueResultMapperFactory<>());
    }

    @Override
    public <T> SingleValueTarantoolResultMapperFactory<T> singleValueTarantoolResultMapperFactory() {
        return new SingleValueTarantoolResultMapperFactory<>();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> SingleValueTarantoolResultMapperFactory<T> singleValueTarantoolResultMapperFactory(
            Class<T> tupleClass) {
        return (SingleValueTarantoolResultMapperFactory<T>) singleTarantoolResultFactoryCache
                .computeIfAbsent(tupleClass, cls -> new SingleValueTarantoolResultMapperFactory<>());
    }

    @Override
    public <T, R extends List<T>> MultiValueResultMapperFactory<T, R> multiValueResultMapperFactory() {
        return new MultiValueResultMapperFactory<>();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, R extends List<T>> MultiValueResultMapperFactory<T, R> multiValueResultMapperFactory(
            Class<R> resultClass) {
        return (MultiValueResultMapperFactory<T, R>) multiResultFactoryCache
                .computeIfAbsent(resultClass, cls -> new MultiValueResultMapperFactory<>());
    }

    @Override
    public <T> MultiValueTarantoolResultMapperFactory<T> multiValueTarantoolResultMapperFactory() {
        return new MultiValueTarantoolResultMapperFactory<>();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> MultiValueTarantoolResultMapperFactory<T> multiValueTarantoolResultMapperFactory(
            Class<? extends TarantoolResult<T>> tupleClass) {
        return (MultiValueTarantoolResultMapperFactory<T>) multiTarantoolResultFactoryCache
                .computeIfAbsent(tupleClass, cls -> new MultiValueTarantoolResultMapperFactory<>());
    }
}
