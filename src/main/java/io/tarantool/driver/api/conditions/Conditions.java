package io.tarantool.driver.api.conditions;

import io.tarantool.driver.api.TarantoolIndexQuery;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.metadata.TarantoolFieldMetadata;
import io.tarantool.driver.metadata.TarantoolIndexMetadata;
import io.tarantool.driver.metadata.TarantoolIndexPartMetadata;
import io.tarantool.driver.metadata.TarantoolSpaceMetadataOperations;
import io.tarantool.driver.protocol.TarantoolIteratorType;
import org.springframework.util.Assert;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * A collection and a builder for tuple filtering conditions.
 * See <a href="https://github.com/tarantool/crud#select-conditions">
 * https://github.com/tarantool/crud#select-conditions</a>
 *
 * @author Alexey Kuzin
 */
public final class Conditions {

    private static final long MAX_LIMIT = 0xff_ff_ff_ffL;
    private static final long MAX_OFFSET = 0xff_ff_ff_ffL;

    private final List<Condition> conditions = new LinkedList<>();

    private boolean descending;
    private long limit = MAX_LIMIT; // 0 is unlimited
    private long offset; // 0 is no offset
    private TarantoolTuple afterTuple;

    private Conditions(boolean descending) {
        this.descending = descending;
    }

    private Conditions(Condition condition) {
        conditions.add(condition);
    }

    private Conditions(long limit, long offset) {
        this.limit = limit;
        this.offset = offset;
    }

    private Conditions(TarantoolTuple afterTuple) {
        this.afterTuple = afterTuple;
    }

    private Conditions(Conditions source) {
        this.conditions.addAll(source.conditions);
        this.descending = source.descending;
        this.limit = source.limit;
        this.offset = source.offset;
        this.afterTuple = source.afterTuple;
    }

    /**
     * Create copy of conditions object
     *
     * @param source a instance of {@link Conditions} for copy
     * @return new {@link Conditions} instance
     */
    public static Conditions clone(Conditions source) {
        return new Conditions(source);
    }

    /**
     * Clear current conditions list
     *
     * @return this {@link Conditions} instance
     */
    public Conditions clearConditions() {
        this.conditions.clear();
        return this;
    }

    /**
     * Get the descending option value
     *
     * @return false by default
     */
    public boolean isDescending() {
        return descending;
    }

    /**
     * Create new Conditions instance, returning tuples in the descending order
     *
     * @return new {@link Conditions} instance
     */
    public static Conditions descending() {
        return new Conditions(true);
    }

    /**
     * Return tuples in the descending order
     *
     * @return this {@link Conditions} instance
     */
    public Conditions withDescending() {
        this.descending = true;
        return this;
    }

    /**
     * Create new Conditions instance, returning tuples will in the ascending order
     *
     * @return new {@link Conditions} instance
     */
    public static Conditions ascending() {
        return new Conditions(false);
    }

    /**
     * Return tuples will in the ascending order
     *
     * @return this {@link Conditions} instance
     */
    public Conditions withAscending() {
        return this;
    }

    /**
     * Create new Conditions instance without any filtration.
     *
     * @return new {@link Conditions} instance
     */
    public static Conditions any() {
        return ascending();
    }

    /**
     * Limit the number od returned tuples with the specified value
     *
     * @param limit number of tuples, should be greater than 0
     * @return new {@link Conditions} instance
     */
    public static Conditions limit(long limit) {
        Assert.state(limit >= 0 && limit <= MAX_LIMIT, "Limit mast be a value between 0 and 0xffffffff");

        return new Conditions(limit, 0);
    }

    /**
     * Limit the number od returned tuples with the specified value
     *
     * @param limit number of tuples, should be greater than 0
     * @return this {@link Conditions} instance
     */
    public Conditions withLimit(long limit) {
        Assert.state(limit >= 0 && limit <= MAX_LIMIT, "Limit mast be a value between 0 and 0xffffffff");

        this.limit = limit;
        return this;
    }

    /**
     * Get the specified limit
     *
     * @return number of tuples
     */
    public long getLimit() {
        return limit;
    }

    /**
     * Skip the specified number of tuples before collecting the result.
     *
     * @param offset number of tuples, should be greater than 0
     * @return new {@link Conditions} instance
     */
    public static Conditions offset(long offset) {
        Assert.state(offset >= 0 && offset <= MAX_OFFSET, "Offset mast be a value between 0 and 0xffffffff");

        return new Conditions(0, offset);
    }

    /**
     * Skip the specified number of tuples before collecting the result.
     *
     * @param offset number of tuples, should be greater than 0
     * @return this {@link Conditions} instance
     */
    public Conditions withOffset(long offset) {
        Assert.state(offset >= 0 && offset <= MAX_OFFSET, "Offset mast be a value between 0 and 0xffffffff");

        this.offset = offset;
        return this;
    }

    /**
     * Get the specified offset
     *
     * @return number of tuples
     */
    public long getOffset() {
        return offset;
    }

    /**
     * The tuple after which objects should be selected.
     *
     * @param tuple a {@link TarantoolTuple}
     * @return new {@link Conditions} instance
     */
    public static Conditions after(TarantoolTuple tuple) {
        Assert.notNull(tuple, "After tuple value should not be null");

        return new Conditions(tuple);
    }

    /**
     * Specified the tuple after which objects should be selected.
     *
     * @param tuple a {@link TarantoolTuple}
     * @return new {@link Conditions} instance
     */
    public Conditions startAfter(TarantoolTuple tuple) {
        Assert.notNull(tuple, "After tuple value should not be null");

        this.afterTuple = tuple;
        return this;
    }

    /**
     * Get the tuple after which objects should be selected.
     *
     * @return list of index parts values
     */
    public TarantoolTuple getAfterTuple() {
        return afterTuple;
    }

    /**
     * Create new Conditions instance with filter by the specified index
     *
     * @param indexName index name
     * @param indexPartValues index parts values
     * @return new {@link Conditions} instance
     */
    public static Conditions indexEquals(String indexName, List<Object> indexPartValues) {
        return new Conditions(new IndexValueCondition(Operator.EQ, new NamedIndex(indexName), indexPartValues));
    }

    /**
     * Filter tuples by the specified index
     *
     * @param indexName index name
     * @param indexPartValues index parts values
     * @return new {@link Conditions} instance
     */
    public Conditions andIndexEquals(String indexName, List<Object> indexPartValues) {
        conditions.add(new IndexValueCondition(Operator.EQ, new NamedIndex(indexName), indexPartValues));
        return this;
    }

    /**
     * Create new Conditions instance with filter by the specified index
     *
     * @param indexId index id
     * @param indexPartValues index parts values
     * @return new {@link Conditions} instance
     */
    public static Conditions indexEquals(int indexId, List<Object> indexPartValues) {
        return new Conditions(new IndexValueCondition(Operator.EQ, new IdIndex(indexId), indexPartValues));
    }

    /**
     * Filter tuples by the specified index
     *
     * @param indexId index id
     * @param indexPartValues index parts values
     * @return this {@link Conditions} instance
     */
    public Conditions andIndexEquals(int indexId, List<Object> indexPartValues) {
        conditions.add(new IndexValueCondition(Operator.EQ, new IdIndex(indexId), indexPartValues));
        return this;
    }

    /**
     * Create new Conditions instance with filter by the specified index, with values greater than the specified value
     *
     * @param indexName index name
     * @param indexPartValues index parts values
     * @return new {@link Conditions} instance
     */
    public static Conditions indexGreaterThan(String indexName, List<Object> indexPartValues) {
        return new Conditions(new IndexValueCondition(Operator.GT, new NamedIndex(indexName), indexPartValues));
    }

    /**
     * Filter tuples by the specified index, with values greater than the specified value
     *
     * @param indexName index name
     * @param indexPartValues index parts values
     * @return new {@link Conditions} instance
     */
    public Conditions andIndexGreaterThan(String indexName, List<Object> indexPartValues) {
        conditions.add(new IndexValueCondition(Operator.GT, new NamedIndex(indexName), indexPartValues));
        return this;
    }

    /**
     * Create new Conditions instance with filter by the specified index, with values greater than the specified value
     *
     * @param indexId index id
     * @param indexPartValues index parts values
     * @return new {@link Conditions} instance
     */
    public static Conditions indexGreaterThan(int indexId, List<Object> indexPartValues) {
        return new Conditions(new IndexValueCondition(Operator.GT, new IdIndex(indexId), indexPartValues));
    }

    /**
     * Filter tuples by the specified index, with values greater than the specified value
     *
     * @param indexId index id
     * @param indexPartValues index parts values
     * @return this {@link Conditions} instance
     */
    public Conditions andIndexGreaterThan(int indexId, List<Object> indexPartValues) {
        conditions.add(new IndexValueCondition(Operator.GT, new IdIndex(indexId), indexPartValues));
        return this;
    }

    /**
     * Create new Conditions instance with filter by the specified index, with values greater or equal than the
     * specified value
     *
     * @param indexName index name
     * @param indexPartValues index parts values
     * @return new {@link Conditions} instance
     */
    public static Conditions indexGreaterOrEquals(String indexName, List<Object> indexPartValues) {
        return new Conditions(new IndexValueCondition(Operator.GE, new NamedIndex(indexName), indexPartValues));
    }

    /**
     * Filter tuples by the specified index, with values greater or equal than the specified value
     *
     * @param indexName index name
     * @param indexPartValues index parts values
     * @return new {@link Conditions} instance
     */
    public Conditions andIndexGreaterOrEquals(String indexName, List<Object> indexPartValues) {
        conditions.add(new IndexValueCondition(Operator.GE, new NamedIndex(indexName), indexPartValues));
        return this;
    }

    /**
     * Create new Conditions instance with filter by the specified index, with values greater or equal than the
     * specified value
     *
     * @param indexId index id
     * @param indexPartValues index parts values
     * @return new {@link Conditions} instance
     */
    public static Conditions indexGreaterOrEquals(int indexId, List<Object> indexPartValues) {
        return new Conditions(new IndexValueCondition(Operator.GE, new IdIndex(indexId), indexPartValues));
    }

    /**
     * Filter tuples by the specified index, with values greater or equal than the specified value
     *
     * @param indexId index id
     * @param indexPartValues index parts values
     * @return this {@link Conditions} instance
     */
    public Conditions andIndexGreaterOrEquals(int indexId, List<Object> indexPartValues) {
        conditions.add(new IndexValueCondition(Operator.GE, new IdIndex(indexId), indexPartValues));
        return this;
    }

    /**
     * Create new Conditions instance with filter by the specified index, with values less than the specified value
     *
     * @param indexName index name
     * @param indexPartValues index parts values
     * @return new {@link Conditions} instance
     */
    public static Conditions indexLessThan(String indexName, List<Object> indexPartValues) {
        return new Conditions(new IndexValueCondition(Operator.LT, new NamedIndex(indexName), indexPartValues));
    }

    /**
     * Filter tuples by the specified index, with values less than the specified value
     *
     * @param indexName index name
     * @param indexPartValues index parts values
     * @return new {@link Conditions} instance
     */
    public Conditions andIndexLessThan(String indexName, List<Object> indexPartValues) {
        conditions.add(new IndexValueCondition(Operator.LT, new NamedIndex(indexName), indexPartValues));
        return this;
    }

    /**
     * Create new Conditions instance with filter by the specified index, with values less than the specified value
     *
     * @param indexId index id
     * @param indexPartValues index parts values
     * @return new {@link Conditions} instance
     */
    public static Conditions indexLessThan(int indexId, List<Object> indexPartValues) {
        return new Conditions(new IndexValueCondition(Operator.LT, new IdIndex(indexId), indexPartValues));
    }

    /**
     * Filter tuples by the specified index, with values less than the specified value
     *
     * @param indexId index id
     * @param indexPartValues index parts values
     * @return this {@link Conditions} instance
     */
    public Conditions andIndexLessThan(int indexId, List<Object> indexPartValues) {
        conditions.add(new IndexValueCondition(Operator.LT, new IdIndex(indexId), indexPartValues));
        return this;
    }

    /**
     * Create new Conditions instance with filter by the specified index, with values less or equal than the
     * specified value
     *
     * @param indexName index name
     * @param indexPartValues index parts values
     * @return new {@link Conditions} instance
     */
    public static Conditions indexLessOrEquals(String indexName, List<Object> indexPartValues) {
        return new Conditions(new IndexValueCondition(Operator.LE, new NamedIndex(indexName), indexPartValues));
    }

    /**
     * Filter tuples by the specified index, with values less or equal than the specified value
     *
     * @param indexName index name
     * @param indexPartValues index parts values
     * @return new {@link Conditions} instance
     */
    public Conditions andIndexLessOrEquals(String indexName, List<Object> indexPartValues) {
        conditions.add(new IndexValueCondition(Operator.LE, new NamedIndex(indexName), indexPartValues));
        return this;
    }

    /**
     * Create new Conditions instance with filter by the specified index, with values less or equal than the
     * specified value
     *
     * @param indexId index id
     * @param indexPartValues index parts values
     * @return new {@link Conditions} instance
     */
    public static Conditions indexLessOrEquals(int indexId, List<Object> indexPartValues) {
        return new Conditions(new IndexValueCondition(Operator.LE, new IdIndex(indexId), indexPartValues));
    }

    /**
     * Filter tuples by the specified index, with values less or equal than the specified value
     *
     * @param indexId index id
     * @param indexPartValues index parts values
     * @return this {@link Conditions} instance
     */
    public Conditions andIndexLessOrEquals(int indexId, List<Object> indexPartValues) {
        conditions.add(new IndexValueCondition(Operator.LE, new IdIndex(indexId), indexPartValues));
        return this;
    }

    /**
     * Filter tuples by the specified field
     *
     * @param fieldName field name
     * @param value field value
     * @return new {@link Conditions} instance
     */
    public static Conditions equals(String fieldName, Object value) {
        return new Conditions(new FieldValueCondition(Operator.EQ, new NamedField(fieldName), value));
    }

    /**
     * Filter tuples by the specified field
     *
     * @param fieldName field name
     * @param value field value
     * @return this {@link Conditions} instance
     */
    public Conditions andEquals(String fieldName, Object value) {
        conditions.add(new FieldValueCondition(Operator.EQ, new NamedField(fieldName), value));
        return this;
    }

    /**
     * Filter tuples by the specified field
     *
     * @param fieldPosition field position
     * @param value field value
     * @return new {@link Conditions} instance
     */
    public static Conditions equals(int fieldPosition, Object value) {
        return new Conditions(new FieldValueCondition(Operator.EQ, new PositionField(fieldPosition), value));
    }

    /**
     * Filter tuples by the specified field
     *
     * @param fieldPosition field position
     * @param value field value
     * @return this {@link Conditions} instance
     */
    public Conditions andEquals(int fieldPosition, Object value) {
        conditions.add(new FieldValueCondition(Operator.EQ, new PositionField(fieldPosition), value));
        return this;
    }

    /**
     * Filter tuples by the specified field, with values greater than the specified value
     *
     * @param fieldName field name
     * @param value field value
     * @return new {@link Conditions} instance
     */
    public static Conditions greaterThan(String fieldName, Object value) {
        return new Conditions(new FieldValueCondition(Operator.GT, new NamedField(fieldName), value));
    }

    /**
     * Filter tuples by the specified field, with values greater than the specified value
     *
     * @param fieldName field name
     * @param value field value
     * @return this {@link Conditions} instance
     */
    public Conditions andGreaterThan(String fieldName, Object value) {
        conditions.add(new FieldValueCondition(Operator.GT, new NamedField(fieldName), value));
        return this;
    }

    /**
     * Filter tuples by the specified field, with values greater than the specified value
     *
     * @param fieldPosition field position
     * @param value field value
     * @return new {@link Conditions} instance
     */
    public static Conditions greaterThan(int fieldPosition, Object value) {
        return new Conditions(new FieldValueCondition(Operator.GT, new PositionField(fieldPosition), value));
    }

    /**
     * Filter tuples by the specified field, with values greater than the specified value
     *
     * @param fieldPosition field position
     * @param value field value
     * @return this {@link Conditions} instance
     */
    public Conditions andGreaterThan(int fieldPosition, Object value) {
        conditions.add(new FieldValueCondition(Operator.GT, new PositionField(fieldPosition), value));
        return this;
    }

    /**
     * Filter tuples by the specified field, with values greater or equal than the specified value
     *
     * @param fieldName field name
     * @param value field value
     * @return new {@link Conditions} instance
     */
    public static Conditions greaterOrEquals(String fieldName, Object value) {
        return new Conditions(new FieldValueCondition(Operator.GE, new NamedField(fieldName), value));
    }

    /**
     * Filter tuples by the specified field, with values greater or equal than the specified value
     *
     * @param fieldName field name
     * @param value field value
     * @return this {@link Conditions} instance
     */
    public Conditions andGreaterOrEquals(String fieldName, Object value) {
        conditions.add(new FieldValueCondition(Operator.GE, new NamedField(fieldName), value));
        return this;
    }

    /**
     * Filter tuples by the specified field, with values greater or equal than the specified value
     *
     * @param fieldPosition field position
     * @param value field value
     * @return new {@link Conditions} instance
     */
    public static Conditions greaterOrEquals(int fieldPosition, Object value) {
        return new Conditions(new FieldValueCondition(Operator.GE, new PositionField(fieldPosition), value));
    }

    /**
     * Filter tuples by the specified field, with values greater or equal than the specified value
     *
     * @param fieldPosition field position
     * @param value field value
     * @return this {@link Conditions} instance
     */
    public Conditions andGreaterOrEquals(int fieldPosition, Object value) {
        conditions.add(new FieldValueCondition(Operator.GE, new PositionField(fieldPosition), value));
        return this;
    }

    /**
     * Filter tuples by the specified field, with values less than the specified value
     *
     * @param fieldName field name
     * @param value field value
     * @return new {@link Conditions} instance
     */
    public static Conditions lessThan(String fieldName, Object value) {
        return new Conditions(new FieldValueCondition(Operator.LT, new NamedField(fieldName), value));
    }

    /**
     * Filter tuples by the specified field, with values less than the specified value
     *
     * @param fieldName field name
     * @param value field value
     * @return this {@link Conditions} instance
     */
    public Conditions andLessThan(String fieldName, Object value) {
        conditions.add(new FieldValueCondition(Operator.LT, new NamedField(fieldName), value));
        return this;
    }

    /**
     * Filter tuples by the specified field, with values less than the specified value
     *
     * @param fieldPosition field position
     * @param value field value
     * @return new {@link Conditions} instance
     */
    public static Conditions lessThan(int fieldPosition, Object value) {
        return new Conditions(new FieldValueCondition(Operator.LT, new PositionField(fieldPosition), value));
    }

    /**
     * Filter tuples by the specified field, with values less than the specified value
     *
     * @param fieldPosition field position
     * @param value field value
     * @return this {@link Conditions} instance
     */
    public Conditions andLessThan(int fieldPosition, Object value) {
        conditions.add(new FieldValueCondition(Operator.LT, new PositionField(fieldPosition), value));
        return this;
    }

    /**
     * Filter tuples by the specified field, with values less or equal than the specified value
     *
     * @param fieldName field name
     * @param value field value
     * @return new {@link Conditions} instance
     */
    public static Conditions lessOrEquals(String fieldName, Object value) {
        return new Conditions(new FieldValueCondition(Operator.LE, new NamedField(fieldName), value));
    }

    /**
     * Filter tuples by the specified field, with values less or equal than the specified value
     *
     * @param fieldName field name
     * @param value field value
     * @return this {@link Conditions} instance
     */
    public Conditions andLessOrEquals(String fieldName, Object value) {
        conditions.add(new FieldValueCondition(Operator.LE, new NamedField(fieldName), value));
        return this;
    }

    /**
     * Filter tuples by the specified field, with values less or equal than the specified value
     *
     * @param fieldPosition field position
     * @param value field value
     * @return new {@link Conditions} instance
     */
    public static Conditions lessOrEquals(int fieldPosition, Object value) {
        return new Conditions(new FieldValueCondition(Operator.LE, new PositionField(fieldPosition), value));
    }

    /**
     * Filter tuples by the specified field, with values less or equal than the specified value
     *
     * @param fieldPosition field position
     * @param value field value
     * @return this {@link Conditions} instance
     */
    public Conditions andLessOrEquals(int fieldPosition, Object value) {
        conditions.add(new FieldValueCondition(Operator.LE, new PositionField(fieldPosition), value));
        return this;
    }

    public List<?> toProxyQuery(TarantoolSpaceMetadataOperations operations) {
        if (offset > 0) {
            throw new TarantoolClientException("Offset is not supported");
        }

        final Map<String, List<IndexValueCondition>> indexConditions = new HashMap<>();
        final Map<Integer, List<FieldValueCondition>> fieldConditions = new HashMap<>();
        final Map<Integer, TarantoolFieldMetadata> selectedFields = new HashMap<>();

        for (Condition condition : conditions) {
            if (condition instanceof IndexValueCondition) {
                TarantoolIndexMetadata indexMetadata =
                        (TarantoolIndexMetadata) condition.field().metadata(operations);

                List<IndexValueCondition> current = indexConditions.computeIfAbsent(
                        indexMetadata.getIndexName(), name -> new LinkedList<>());

                current.add(convertIndexIfNecessary((IndexValueCondition) condition, indexMetadata.getIndexName()));
            } else {
                TarantoolFieldMetadata fieldMetadata =
                        (TarantoolFieldMetadata) condition.field().metadata(operations);
                List<FieldValueCondition> current = fieldConditions
                        .computeIfAbsent(fieldMetadata.getFieldPosition(), f -> new LinkedList<>());

                current.add((FieldValueCondition) condition);
                selectedFields.putIfAbsent(fieldMetadata.getFieldPosition(), fieldMetadata);
            }
        }

//        if (indexConditions.size() > 1) {
//            throw new TarantoolClientException("Filtering by more than one index is not supported");
//        }

        List<List<Object>> allConditions = new ArrayList<>();
        if (indexConditions.size() > 0) {
            allConditions.addAll(
                    conditionsListToLists(indexConditions.values().iterator().next(), operations));
            for (List<FieldValueCondition> conditionList : fieldConditions.values()) {
                allConditions.addAll(conditionsListToLists(conditionList, operations));
            }
        } else {
            Optional<TarantoolIndexMetadata> suitableIndex = findCoveringIndex(
                    operations, selectedFields.values());

            if (suitableIndex.isPresent()) {
                for (TarantoolIndexPartMetadata part : suitableIndex.get().getIndexParts()) {
                    List<FieldValueCondition> conditions = fieldConditions.get(part.getFieldIndex());
                    if (conditions != null) {
                        allConditions.addAll(conditionsListToLists(conditions, operations));
                        fieldConditions.remove(part.getFieldIndex());
                    }
                }
            }

            for (List<FieldValueCondition> conditionList : fieldConditions.values()) {
                allConditions.addAll(conditionsListToLists(conditionList, operations));
            }
        }

        return allConditions;
    }

    private IndexValueCondition convertIndexIfNecessary(IndexValueCondition condition,
                                                        String indexName) {
        if (!(condition.field() instanceof IdIndex)) {
            return condition;
        }
        return new IndexValueCondition(condition.operator(), new NamedIndex(indexName), condition.value());
    }

    private List<List<Object>> conditionsListToLists(List<? extends Condition> conditionsList,
                                                     TarantoolSpaceMetadataOperations operations) {
        return conditionsList.stream().map(c -> c.toList(operations)).collect(Collectors.toList());
    }

    public TarantoolIndexQuery toIndexQuery(TarantoolSpaceMetadataOperations operations) {
        if (afterTuple != null) {
            throw new TarantoolClientException("'startAfter' is not supported");
        }

        final Map<String, List<IndexValueCondition>> indexConditions = new HashMap<>();
        final Map<String, TarantoolIndexMetadata> selectedIndexes = new HashMap<>();
        final Map<String, List<FieldValueCondition>> fieldConditions = new HashMap<>();
        final Map<String, TarantoolFieldMetadata> selectedFields = new HashMap<>();

        for (Condition condition : conditions) {
            if (condition instanceof IndexValueCondition) {
                TarantoolIndexMetadata indexMetadata =
                        (TarantoolIndexMetadata) condition.field().metadata(operations);
                List<IndexValueCondition> current = indexConditions.computeIfAbsent(
                        indexMetadata.getIndexName(), name -> new LinkedList<>());

                if (current.size() > 0) {
                    throw new TarantoolClientException("Multiple conditions for one index are not supported");
                }

                current.add((IndexValueCondition) condition);
                selectedIndexes.putIfAbsent(indexMetadata.getIndexName(), indexMetadata);
            } else {
                TarantoolFieldMetadata fieldMetadata =
                        (TarantoolFieldMetadata) condition.field().metadata(operations);
                List<FieldValueCondition> current = fieldConditions
                        .computeIfAbsent(fieldMetadata.getFieldName(), f -> new LinkedList<>());

                if (current.size() > 0) {
                    throw new TarantoolClientException("Multiple conditions for one field are not supported");
                }

                current.add((FieldValueCondition) condition);
                selectedFields.putIfAbsent(fieldMetadata.getFieldName(), fieldMetadata);
            }
        }

        if (indexConditions.size() > 1) {
            throw new TarantoolClientException("Filtering by more than one index is not supported");
        }

        TarantoolIndexQuery query;

        if (indexConditions.size() > 0) {

            if (fieldConditions.size() > 0) {
                throw new TarantoolClientException("Filtering simultaneously by index and fields is not supported");
            }

            query = indexQueryFromIndexValues(indexConditions, selectedIndexes);
        } else {
            if (selectedFields.size() > 0) {
                TarantoolIndexMetadata suitableIndex = findSuitableIndex(
                        operations, selectedFields.values());

                query = indexQueryFromFieldValues(suitableIndex, fieldConditions, selectedFields);
            } else {
                query = new TarantoolIndexQuery(TarantoolIndexQuery.PRIMARY)
                        .withIteratorType(descending ? TarantoolIteratorType.ITER_REQ : TarantoolIteratorType.ITER_EQ);
            }
        }

        return query;
    }

    private TarantoolIndexQuery indexQueryFromIndexValues(Map<String, List<IndexValueCondition>> indexConditions,
                                                          Map<String, TarantoolIndexMetadata> selectedIndexes) {
        IndexValueCondition condition = indexConditions.values().iterator().next().get(0);
        TarantoolIndexMetadata indexMetadata = selectedIndexes.values().iterator().next();
        TarantoolIteratorType iteratorType = condition.operator().toIteratorType();
        return new TarantoolIndexQuery(indexMetadata.getIndexId())
                .withIteratorType(descending ? iteratorType.reverse() : iteratorType)
                .withKeyValues(condition.value());
    }

    private TarantoolIndexQuery indexQueryFromFieldValues(TarantoolIndexMetadata suitableIndex,
                                                          Map<String, List<FieldValueCondition>> fieldConditions,
                                                          Map<String, TarantoolFieldMetadata> selectedFields) {
        Operator selectedOperator = null;
        List<Object> fieldValues = Arrays.asList(new Object[suitableIndex.getIndexParts().size()]);
        for (Map.Entry<String, List<FieldValueCondition>> conditions : fieldConditions.entrySet()) {
            FieldValueCondition condition = conditions.getValue().iterator().next();
            if (selectedOperator == null) {
                selectedOperator = condition.operator();
            } else {
                if (!condition.operator().equals(selectedOperator)) {
                    throw new TarantoolClientException(
                            "Different conditions for index parts are not supported");
                }
            }
            TarantoolFieldMetadata field = selectedFields.get(conditions.getKey());
            int partPosition = suitableIndex
                    .getIndexPartPositionByFieldPosition(field.getFieldPosition())
                    .orElseThrow(() -> new TarantoolClientException(
                            "Field %s not found in index %s", field.getFieldName(), suitableIndex.getIndexName()));
            fieldValues.set(partPosition, condition.value());
        }
        TarantoolIteratorType iteratorType = selectedOperator != null ?
                selectedOperator.toIteratorType() :
                TarantoolIteratorType.ITER_EQ;
        return new TarantoolIndexQuery(suitableIndex.getIndexId())
                .withIteratorType(descending ? iteratorType.reverse() : iteratorType)
                .withKeyValues(fieldValues);
    }

    private static Optional<TarantoolIndexMetadata> findCoveringIndex(TarantoolSpaceMetadataOperations operations,
                                                                  Collection<TarantoolFieldMetadata> selectedFields) {
        Map<String, TarantoolIndexMetadata> allIndexes = operations.getSpaceIndexes();

        Optional<TarantoolIndexMetadata> coveringIndex = allIndexes.values().stream()
                .map(metadata -> new AbstractMap.SimpleEntry<Long, TarantoolIndexMetadata>(
                                calculateCoverage(metadata, selectedFields), metadata))
                .filter(entry -> entry.getKey() > 0)
                .max(Comparator.comparingLong(AbstractMap.SimpleEntry::getKey))
                .map(AbstractMap.SimpleEntry::getValue);

        return coveringIndex;
    }

    private static long calculateCoverage(TarantoolIndexMetadata metadata,
                                         Collection<TarantoolFieldMetadata> selectedFields) {
        AtomicBoolean firstFieldIsSet = new AtomicBoolean(false);
        long count = selectedFields.stream()
                .map(f -> metadata.getIndexPartPositionByFieldPosition(f.getFieldPosition()))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .peek(indexPosition -> {
                    if (indexPosition == 0) {
                        firstFieldIsSet.set(true);
                    }
                })
                .count();
        return firstFieldIsSet.get() ? count : 0;
    }

    private static TarantoolIndexMetadata findSuitableIndex(TarantoolSpaceMetadataOperations operations,
                                                            Collection<TarantoolFieldMetadata> selectedFields) {
        Map<String, TarantoolIndexMetadata> allIndexes = operations.getSpaceIndexes();
        TarantoolIndexMetadata suitableIndex = allIndexes.values().stream()
                .filter(metadata -> isSuitableIndex(metadata, selectedFields))
                .min(Comparator.comparingInt(m -> m.getIndexParts().size()))
                .orElseThrow(() -> new TarantoolClientException("No indexes that fit the passed fields are found"));

        return suitableIndex;
    }

    private static boolean isSuitableIndex(TarantoolIndexMetadata indexMetadata,
                                           Collection<TarantoolFieldMetadata> selectedFields) {
        Map<Integer, TarantoolIndexPartMetadata> indexParts = indexMetadata.getIndexPartsByPosition();

        if (indexParts.size() < selectedFields.size()) {
            return false;
        }

        for (TarantoolFieldMetadata fieldMetadata : selectedFields) {
            if (!indexParts.containsKey(fieldMetadata.getFieldPosition())) {
                return false;
            }
        }

        return true;
    }
}
