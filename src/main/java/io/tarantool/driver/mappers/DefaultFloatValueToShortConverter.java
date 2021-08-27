package io.tarantool.driver.mappers;

import org.msgpack.value.FloatValue;

/**
 * Default {@link FloatValue} to {@code Short} converter
 *
 * @author Oleg Kuznetsov
 */
public class DefaultFloatValueToShortConverter implements ValueConverter<FloatValue, Short> {

    private static final long serialVersionUID = 20210819L;

    @Override
    public Short fromValue(FloatValue value) {
        return value.toShort();
    }

    @Override
    public boolean canConvertValue(FloatValue value) {
        double aDouble = value.toDouble();
        return aDouble >= Short.MIN_VALUE && aDouble <= Short.MAX_VALUE;
    }
}
