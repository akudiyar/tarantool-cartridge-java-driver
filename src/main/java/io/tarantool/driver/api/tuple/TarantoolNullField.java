package io.tarantool.driver.api.tuple;

import io.tarantool.driver.mappers.MessagePackObjectMapper;
import io.tarantool.driver.mappers.MessagePackValueMapper;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import org.springframework.lang.Nullable;

import java.util.Objects;

/**
 * Represents a field with empty value which serializes to {@code msgpack.NULL}
 *
 * @author Alexey Kuzin
 */
public final class TarantoolNullField implements TarantoolField {

    @Override
    public Value toMessagePackValue(MessagePackObjectMapper mapper) {
        return ValueFactory.newNil();
    }

    @Nullable
    @Override
    public <O> O getValue(Class<O> targetClass, MessagePackValueMapper mapper) {
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        return o instanceof TarantoolNullField;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(this);
    }
}
