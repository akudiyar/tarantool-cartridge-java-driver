package io.tarantool.driver.integration;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.cursor.TarantoolCursorOptions;
import io.tarantool.driver.api.cursor.TarantoolCursor;
import io.tarantool.driver.api.space.TarantoolSpaceOperations;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleImpl;
import io.tarantool.driver.exceptions.TarantoolClientException;
import io.tarantool.driver.mappers.DefaultMessagePackMapperFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Testcontainers
public class StandaloneTarantoolCursorIT extends SharedTarantoolContainer {
    private static final Logger log = LoggerFactory.getLogger(StandaloneTarantoolCursorIT.class);

    private static final String TEST_SPACE_NAME = "cursor_test_space";
    private static final String TEST_MULTIPART_KEY_SPACE_NAME = "cursor_test_space_multi_part_key";

    private static TarantoolClient client;
    private static final DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();

    @BeforeAll
    public static void setUp() {
        assertTrue(tarantoolContainer.isRunning());

        log.info("Attempting connect to Tarantool");
        client = createClient();
        log.info("Successfully connected to Tarantool, version = {}", client.getVersion());

        try {
            insertTestData();
        } catch (ExecutionException | InterruptedException ignored) {
            fail();
        }
    }

    public static void tearDown() throws Exception {
        client.close();
        assertThrows(TarantoolClientException.class, () -> client.metadata().getSpaceByName("_space"));
    }

    private static void insertTestData() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations testSpace = client.space(TEST_SPACE_NAME);
        TarantoolSpaceOperations multipartTestSpace = client.space(TEST_MULTIPART_KEY_SPACE_NAME);

        for (int i = 1; i <= 100; i++) {
            //insert new data
            List<Object> values = Arrays.asList(i, i + "abc", i * 10);
            TarantoolTuple tarantoolTuple = new TarantoolTupleImpl(values, mapperFactory.defaultComplexTypesMapper());
            testSpace.insert(tarantoolTuple).get();

            values = Arrays.asList(i / 10, i + "abc", i * 10);
            tarantoolTuple = new TarantoolTupleImpl(values, mapperFactory.defaultComplexTypesMapper());
            multipartTestSpace.insert(tarantoolTuple);
        }
    }

    @Test
    public void getOneTuple() {
        TarantoolSpaceOperations testSpace = client.space(TEST_SPACE_NAME);

        Conditions conditions = Conditions.indexEquals("primary", Collections.singletonList(1));
        TarantoolCursor<TarantoolTuple> cursor = testSpace.cursor(conditions, new TarantoolCursorOptions());

        assertTrue(cursor.hasNext());
        TarantoolTuple tuple = cursor.next();
        assertFalse(cursor.hasNext());

        assertEquals(1, tuple.getInteger(0));
        assertEquals("1abc", tuple.getString(1));
        assertEquals(10, tuple.getInteger(2));

        assertThrows(NoSuchElementException.class, cursor::next);
    }

    @Test
    public void getTuples_withLimitAndCondition() {
        TarantoolSpaceOperations testSpace = client.space(TEST_SPACE_NAME);

        Conditions conditions = Conditions
                .indexGreaterOrEquals("primary", Collections.singletonList(12))
                .withLimit(13);

        TarantoolCursor<TarantoolTuple> cursor = testSpace.cursor(conditions, new TarantoolCursorOptions(3));

        assertTrue(cursor.hasNext());
        List<Integer> tupleIds = new ArrayList<>();
        int countTotal = 0;
        boolean hasNext;
        do {
            countTotal++;
            TarantoolTuple t = cursor.next();
            tupleIds.add(t.getInteger(0));
            hasNext = cursor.hasNext();
        } while (hasNext);

        assertEquals(13, countTotal);
        assertEquals(Arrays.asList(12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24), tupleIds);
        assertThrows(NoSuchElementException.class, cursor::next);
    }

    @Test
    public void getTuplesWithLimitAndCondition_lessThat() {
        TarantoolSpaceOperations testSpace = client.space(TEST_SPACE_NAME);

        Conditions conditions = Conditions
                .indexLessThan("primary", Collections.singletonList(53))
                .withLimit(14);

        TarantoolCursor<TarantoolTuple> cursor = testSpace.cursor(conditions, new TarantoolCursorOptions(3));

        assertTrue(cursor.hasNext());
        List<Integer> tupleIds = new ArrayList<>();
        int countTotal = 0;
        boolean hasNext;
        do {
            countTotal++;
            TarantoolTuple t = cursor.next();
            tupleIds.add(t.getInteger(0));
            hasNext = cursor.hasNext();
        } while (hasNext);

        assertEquals(14, countTotal);
        assertEquals(Arrays.asList(52, 51, 50, 49, 48, 47, 46, 45, 44, 43, 42, 41, 40, 39), tupleIds);
        assertThrows(NoSuchElementException.class, cursor::next);
    }

    @Test
    public void countAll_batchByOneElement() {
        TarantoolSpaceOperations testSpace = client.space(TEST_SPACE_NAME);
        Conditions conditions = Conditions.any();
        TarantoolCursor<TarantoolTuple> cursor = testSpace.cursor(conditions, new TarantoolCursorOptions(3));

        assertTrue(cursor.hasNext());
        int countTotal = 0;

        boolean hasNext;
        do {
            countTotal++;
            TarantoolTuple t = cursor.next();
            hasNext = cursor.hasNext();
        } while (hasNext);

        assertEquals(100, countTotal);

        cursor = testSpace.cursor(conditions, new TarantoolCursorOptions(1));
        List<Integer> tupleIds = new ArrayList<>();
        assertTrue(cursor.hasNext());
        countTotal = 0;
        do {
            countTotal++;
            TarantoolTuple t = cursor.next();
            tupleIds.add(t.getInteger(0));
            hasNext = cursor.hasNext();
        } while (hasNext);

        assertEquals(100, countTotal);
    }

    @Test
    public void countAll_smallBatch() {
        TarantoolSpaceOperations testSpace = client.space(TEST_SPACE_NAME);
        Conditions conditions = Conditions.any();
        TarantoolCursor<TarantoolTuple> cursor = testSpace.cursor(conditions, new TarantoolCursorOptions(10));

        assertTrue(cursor.hasNext());

        int countTotal = 0;
        do {
            countTotal++;
            cursor.next();
        } while (cursor.hasNext());

        assertEquals(100, countTotal);
    }

    @Test
    public void countAll_batchSizeEqualCount() {
        TarantoolSpaceOperations testSpace = client.space(TEST_SPACE_NAME);
        Conditions conditions = Conditions.any();
        TarantoolCursor<TarantoolTuple> cursor = testSpace.cursor(conditions, new TarantoolCursorOptions(100));

        assertTrue(cursor.hasNext());

        int countTotal = 0;
        do {
            countTotal++;
            cursor.next();
        } while (cursor.hasNext());

        assertEquals(100, countTotal);
    }

    @Test
    public void countAll_largeBatch() {
        TarantoolSpaceOperations testSpace = client.space(TEST_SPACE_NAME);
        Conditions conditions = Conditions.any();
        TarantoolCursor<TarantoolTuple> cursor = testSpace.cursor(conditions, new TarantoolCursorOptions(1000));

        assertTrue(cursor.hasNext());

        int countTotal = 0;
        do {
            countTotal++;
            cursor.next();
        } while (cursor.hasNext());

        assertEquals(100, countTotal);
    }

    @Test
    public void getTupleByPartialKey() {
        TarantoolSpaceOperations testSpace = client.space(TEST_MULTIPART_KEY_SPACE_NAME);

        Conditions conditions = Conditions.indexEquals("primary", Collections.singletonList(3));
        TarantoolCursor<TarantoolTuple> cursor = testSpace.cursor(conditions, new TarantoolCursorOptions(2));

        assertTrue(cursor.hasNext());
        int countTotal = 0;
        boolean hasNext;
        do {
            countTotal++;
            TarantoolTuple t = cursor.next();
            hasNext = cursor.hasNext();
        } while (hasNext);

        assertEquals(10, countTotal);

        assertThrows(NoSuchElementException.class, cursor::next);
    }
}
