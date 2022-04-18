package io.tarantool.driver.mappers;

import io.tarantool.driver.mappers.converters.object.DefaultNilValueToNullConverter;
import io.tarantool.driver.mappers.converters.object.DefaultBigDecimalToExtensionValueConverter;
import io.tarantool.driver.mappers.converters.object.DefaultBooleanToBooleanValueConverter;
import io.tarantool.driver.mappers.converters.object.DefaultByteArrayToBinaryValueConverter;
import io.tarantool.driver.mappers.converters.object.DefaultCharacterToStringValueConverter;
import io.tarantool.driver.mappers.converters.object.DefaultDoubleToFloatValueConverter;
import io.tarantool.driver.mappers.converters.object.DefaultFloatToFloatValueConverter;
import io.tarantool.driver.mappers.converters.object.DefaultIntegerToIntegerValueConverter;
import io.tarantool.driver.mappers.converters.object.DefaultLongToIntegerValueConverter;
import io.tarantool.driver.mappers.converters.object.DefaultPackableObjectConverter;
import io.tarantool.driver.mappers.converters.object.DefaultStringToStringValueConverter;
import io.tarantool.driver.mappers.converters.object.DefaultUUIDToExtensionValueConverter;
import io.tarantool.driver.mappers.converters.value.DefaultBigIntegerValueToLongConverter;
import io.tarantool.driver.mappers.converters.value.DefaultBinaryValueToByteArrayConverter;
import io.tarantool.driver.mappers.converters.value.DefaultBooleanValueToBooleanConverter;
import io.tarantool.driver.mappers.converters.value.DefaultExtensionValueToBigDecimalConverter;
import io.tarantool.driver.mappers.converters.value.DefaultExtensionValueToUUIDConverter;
import io.tarantool.driver.mappers.converters.value.DefaultFloatValueToDoubleConverter;
import io.tarantool.driver.mappers.converters.value.DefaultFloatValueToFloatConverter;
import io.tarantool.driver.mappers.converters.value.DefaultFloatValueToIntegerConverter;
import io.tarantool.driver.mappers.converters.value.DefaultFloatValueToLongConverter;
import io.tarantool.driver.mappers.converters.value.DefaultFloatValueToShortConverter;
import io.tarantool.driver.mappers.converters.value.DefaultIntegerValueToIntegerConverter;
import io.tarantool.driver.mappers.converters.value.DefaultIntegerValueToDoubleConverter;
import io.tarantool.driver.mappers.converters.value.DefaultIntegerValueToFloatConverter;
import io.tarantool.driver.mappers.converters.value.DefaultIntegerValueToLongConverter;
import io.tarantool.driver.mappers.converters.value.DefaultStringValueToCharacterConverter;
import io.tarantool.driver.mappers.converters.value.DefaultStringValueToStringConverter;
import org.msgpack.value.BinaryValue;
import org.msgpack.value.BooleanValue;
import org.msgpack.value.ExtensionValue;
import org.msgpack.value.FloatValue;
import org.msgpack.value.IntegerValue;
import org.msgpack.value.StringValue;
import org.msgpack.value.impl.ImmutableBigIntegerValueImpl;
import org.msgpack.value.impl.ImmutableBinaryValueImpl;
import org.msgpack.value.impl.ImmutableBooleanValueImpl;
import org.msgpack.value.impl.ImmutableDoubleValueImpl;
import org.msgpack.value.impl.ImmutableExtensionValueImpl;
import org.msgpack.value.impl.ImmutableLongValueImpl;
import org.msgpack.value.impl.ImmutableNilValueImpl;
import org.msgpack.value.impl.ImmutableStringValueImpl;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Provides shortcuts for instantiating {@link DefaultMessagePackMapper}
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public final class DefaultMessagePackMapperFactory {

    private static final DefaultMessagePackMapperFactory instance = new DefaultMessagePackMapperFactory();

    private final DefaultMessagePackMapper defaultSimpleTypesMapper;

    /**
     * Basic constructor.
     */
    private DefaultMessagePackMapperFactory() {
        defaultSimpleTypesMapper = new DefaultMessagePackMapper.Builder()
                // converters for primitive values
                .withValueConverter(ImmutableStringValueImpl.class, Character.class,
                        new DefaultStringValueToCharacterConverter())
                .withValueConverter(ImmutableStringValueImpl.class, String.class,
                        new DefaultStringValueToStringConverter())
                .withValueConverter(ImmutableBigIntegerValueImpl.class, Float.class,
                        new DefaultIntegerValueToFloatConverter())
                .withValueConverter(ImmutableBigIntegerValueImpl.class, Double.class,
                        new DefaultIntegerValueToDoubleConverter())
                .withValueConverter(ImmutableBigIntegerValueImpl.class, Long.class,
                        new DefaultBigIntegerValueToLongConverter())
                .withValueConverter(ImmutableBigIntegerValueImpl.class, Integer.class,
                        new DefaultIntegerValueToIntegerConverter())
                .withValueConverter(ImmutableLongValueImpl.class, Float.class,
                        new DefaultIntegerValueToFloatConverter())
                .withValueConverter(ImmutableLongValueImpl.class, Double.class,
                        new DefaultIntegerValueToDoubleConverter())
                .withValueConverter(ImmutableLongValueImpl.class, Long.class,
                        new DefaultIntegerValueToLongConverter())
                .withValueConverter(ImmutableLongValueImpl.class, Integer.class,
                        new DefaultIntegerValueToIntegerConverter())
                .withValueConverter(ImmutableBinaryValueImpl.class, byte[].class,
                        new DefaultBinaryValueToByteArrayConverter())
                .withValueConverter(ImmutableBooleanValueImpl.class, Boolean.class,
                        new DefaultBooleanValueToBooleanConverter())
                .withValueConverter(ImmutableDoubleValueImpl.class, Short.class,
                        new DefaultFloatValueToShortConverter())
                .withValueConverter(ImmutableDoubleValueImpl.class, Long.class,
                        new DefaultFloatValueToLongConverter())
                .withValueConverter(ImmutableDoubleValueImpl.class, Integer.class,
                        new DefaultFloatValueToIntegerConverter())
                .withValueConverter(ImmutableDoubleValueImpl.class, Float.class,
                        new DefaultFloatValueToFloatConverter())
                .withValueConverter(ImmutableDoubleValueImpl.class, Double.class,
                        new DefaultFloatValueToDoubleConverter())
                .withValueConverter(ImmutableExtensionValueImpl.class, UUID.class,
                        new DefaultExtensionValueToUUIDConverter())
                .withValueConverter(ImmutableExtensionValueImpl.class, BigDecimal.class,
                        new DefaultExtensionValueToBigDecimalConverter())
                .withValueConverter(ImmutableNilValueImpl.class, Object.class,
                        new DefaultNilValueToNullConverter())
                //TODO: add this when will it be resolved https://github.com/tarantool/cartridge-java/issues/118
                // .withValueConverter(ImmutableBigIntegerValueImpl.class, Short.class,
                // new DefaultIntegerValueToShortConverter())
                // .withValueConverter(ImmutableLongValueImpl.class, Short.class,
                // new DefaultIntegerValueToShortConverter())
                // .withObjectConverter(Short.class, IntegerValue.class,
                // new DefaultShortToIntegerObjectConverter())
                .withObjectConverter(Character.class, StringValue.class,
                        new DefaultCharacterToStringValueConverter())
                .withObjectConverter(String.class, StringValue.class,
                        new DefaultStringToStringValueConverter())
                .withObjectConverter(Long.class, IntegerValue.class,
                        new DefaultLongToIntegerValueConverter())
                .withObjectConverter(Integer.class, IntegerValue.class,
                        new DefaultIntegerToIntegerValueConverter())
                .withObjectConverter(byte[].class, BinaryValue.class,
                        new DefaultByteArrayToBinaryValueConverter())
                .withObjectConverter(Boolean.class, BooleanValue.class,
                        new DefaultBooleanToBooleanValueConverter())
                .withObjectConverter(Float.class, FloatValue.class,
                        new DefaultFloatToFloatValueConverter())
                .withObjectConverter(Double.class, FloatValue.class,
                        new DefaultDoubleToFloatValueConverter())
                .withObjectConverter(UUID.class, ExtensionValue.class,
                        new DefaultUUIDToExtensionValueConverter())
                .withObjectConverter(BigDecimal.class, ExtensionValue.class,
                        new DefaultBigDecimalToExtensionValueConverter())
                .build();
    }

    /**
     * Get modification-safe instance of the mapper. The instance already contains converters for simple types
     *
     * @return {@link DefaultMessagePackMapper} instance
     */
    public DefaultMessagePackMapper defaultSimpleTypeMapper() {
        // internal types converter
        DefaultMessagePackMapper simpleTypesMapper = new DefaultMessagePackMapper(defaultSimpleTypesMapper);
        simpleTypesMapper.registerObjectConverter(
                new DefaultPackableObjectConverter(simpleTypesMapper));
        return simpleTypesMapper;
    }

    /**
     * Get modification-safe instance of the mapper. The instance contains converters for simple types and complex types
     * {@link java.util.Map} and {@link java.util.List}
     *
     * @return {@link DefaultMessagePackMapper} instance
     */
    public DefaultMessagePackMapper defaultComplexTypesMapper() {
        DefaultMessagePackMapper defaultComplexTypesMapper =
                new DefaultMessagePackMapper.Builder(defaultSimpleTypesMapper)
                        .withDefaultListObjectConverter()
                        .withDefaultArrayValueConverter()
                        .withDefaultMapObjectConverter()
                        .withDefaultMapValueConverter()
                        .build();

        // internal types converter
        defaultComplexTypesMapper.registerObjectConverter(
                new DefaultPackableObjectConverter(defaultComplexTypesMapper));
        return defaultComplexTypesMapper;
    }

    /**
     * Get modification-safe instance of the given mapper (shallow copy).
     *
     * @param mapper configured mapper instance
     * @return new mapper instance
     */
    public DefaultMessagePackMapper copyOf(DefaultMessagePackMapper mapper) {
        return new DefaultMessagePackMapper(mapper);
    }

    /**
     * Get new empty mapper.
     *
     * @return new mapper instance
     */
    public DefaultMessagePackMapper emptyMapper() {
        return new DefaultMessagePackMapper();
    }

    /**
     * Get factory instance.
     *
     * @return factory instance
     */
    public static DefaultMessagePackMapperFactory getInstance() {
        return instance;
    }
}
