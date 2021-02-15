package io.tarantool.driver.mappers;

import io.tarantool.driver.api.MultiValueCallResult;
import org.msgpack.value.ArrayValue;
import org.msgpack.value.Value;

import java.util.List;

/**
 * Factory for {@link CallResultMapper} instances used for handling Lua call multi-return result which is
 * treated as a list of values
 *
 * @author Alexey Kuzin
 */
public class MultiValueResultMapperFactory<T, R extends List<T>> extends
        TarantoolCallResultMapperFactory<R, MultiValueCallResult<T, R>> {

    public MultiValueResultMapperFactory(MessagePackMapper messagePackMapper) {
        super(messagePackMapper);
    }

    /**
     * Get result mapper for the Lua function call with multi-return result
     *
     * @param itemsConverter the result list converter
     * @return call result mapper
     */
    public CallResultMapper<R, MultiValueCallResult<T, R>> withMultiValueResultConverter(
            ValueConverter<ArrayValue, R> itemsConverter) {
        return withConverter(new MultiValueCallResultConverter<>(itemsConverter));
    }

    /**
     * Get result mapper for the Lua function call with multi-return result
     *
     * @param itemsConverter result list converter
     * @param resultClass full result type class
     * @return call result mapper
     */
    public CallResultMapper<R, MultiValueCallResult<T, R>> withMultiValueResultConverter(
            ValueConverter<ArrayValue, R> itemsConverter,
            Class<? extends MultiValueCallResult<T, R>> resultClass) {
        return withConverter(resultClass, new MultiValueCallResultConverter<>(itemsConverter));
    }
}