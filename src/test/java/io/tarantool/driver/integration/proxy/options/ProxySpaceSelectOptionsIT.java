package io.tarantool.driver.integration.proxy.options;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientConfig;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.conditions.Conditions;
import io.tarantool.driver.api.space.options.enums.Mode;
import io.tarantool.driver.api.space.options.interfaces.SelectOptions;
import io.tarantool.driver.api.space.options.interfaces.TarantoolSpaceOperations;
import io.tarantool.driver.api.space.options.proxy.ProxySelectOptions;
import io.tarantool.driver.api.tuple.DefaultTarantoolTupleFactory;
import io.tarantool.driver.api.tuple.TarantoolTuple;
import io.tarantool.driver.api.tuple.TarantoolTupleFactory;
import io.tarantool.driver.auth.SimpleTarantoolCredentials;
import io.tarantool.driver.core.ClusterTarantoolTupleClient;
import io.tarantool.driver.core.ProxyTarantoolTupleClient;
import io.tarantool.driver.integration.SharedCartridgeContainer;
import io.tarantool.driver.mappers.factories.DefaultMessagePackMapperFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * @author Artyom Dubinin
 */
public class ProxySpaceSelectOptionsIT extends SharedCartridgeContainer {

    private static final DefaultMessagePackMapperFactory mapperFactory = DefaultMessagePackMapperFactory.getInstance();
    private static final TarantoolTupleFactory tupleFactory =
        new DefaultTarantoolTupleFactory(mapperFactory.defaultComplexTypesMapper());
    private static final String TEST_SPACE_NAME = "test__profile";
    private static final String PK_FIELD_NAME = "profile_id";
    public static String USER_NAME;
    public static String PASSWORD;
    private static TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client;

    @BeforeAll
    public static void setUp() throws Exception {
        startCluster();
        USER_NAME = container.getUsername();
        PASSWORD = container.getPassword();
        initClient();
    }

    private static void initClient() {
        TarantoolClientConfig config =
            TarantoolClientConfig.builder().withCredentials(new SimpleTarantoolCredentials(USER_NAME, PASSWORD))
                .withConnectTimeout(1000).withReadTimeout(1000).build();

        ClusterTarantoolTupleClient clusterClient =
            new ClusterTarantoolTupleClient(config, container.getRouterHost(), container.getRouterPort());
        client = new ProxyTarantoolTupleClient(clusterClient);
    }

    private static void truncateSpace(String spaceName) {
        client.space(spaceName).truncate().join();
    }

    @BeforeEach
    public void truncateSpace() {
        truncateSpace(TEST_SPACE_NAME);
    }

    @Test
    public void withBatchSizeTest() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace =
            client.space(TEST_SPACE_NAME);

        TarantoolTuple tarantoolTuple;

        for (int i = 0; i < 100; i++) {
            tarantoolTuple = tupleFactory.create(i, null, "FIO", i, i);
            profileSpace.insert(tarantoolTuple).get();
        }

        Conditions conditions = Conditions.greaterOrEquals(PK_FIELD_NAME, 0).withLimit(10);

        // without batchSize
        TarantoolResult<TarantoolTuple> selectResult = profileSpace.select(conditions).get();
        assertEquals(10, selectResult.size());
        List<?> crudSelectOpts = client.eval("return crud_select_opts").get();
        assertNull(((HashMap) crudSelectOpts.get(0)).get("batch_size"));

        // with batchSize
        selectResult = profileSpace.select(conditions, ProxySelectOptions.create().withBatchSize(5)).get();
        assertEquals(10, selectResult.size());
        crudSelectOpts = client.eval("return crud_select_opts").get();
        assertEquals(5, ((HashMap) crudSelectOpts.get(0)).get("batch_size"));
    }

    @Test
    public void withTimeoutTest() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace =
            client.space(TEST_SPACE_NAME);

        int requestConfigTimeout = client.getConfig().getRequestTimeout();
        int customRequestTimeout = requestConfigTimeout * 2;

        // with config timeout
        profileSpace.select(Conditions.any()).get();
        List<?> crudSelectOpts = client.eval("return crud_select_opts").get();
        assertNull(((HashMap) crudSelectOpts.get(0)).get("timeout"));

        profileSpace.select(Conditions.any(), ProxySelectOptions.create()).get();
        crudSelectOpts = client.eval("return crud_select_opts").get();
        assertNull(((HashMap) crudSelectOpts.get(0)).get("timeout"));

        // with option timeout
        profileSpace.select(Conditions.any(), ProxySelectOptions.create().withTimeout(customRequestTimeout)).get();
        crudSelectOpts = client.eval("return crud_select_opts").get();
        assertEquals(customRequestTimeout, ((HashMap) crudSelectOpts.get(0)).get("timeout"));
    }

    @Test
    public void withFieldsTest() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> profileSpace =
            client.space(TEST_SPACE_NAME);

        TarantoolTuple tarantoolTuple;

        tarantoolTuple = tupleFactory.create(1, null, "FIO", 50, 100);
        profileSpace.insert(tarantoolTuple).get();

        Conditions conditions = Conditions.equals(PK_FIELD_NAME, 1);

        // without fields
        TarantoolResult<TarantoolTuple> selectResult = profileSpace.select(conditions).get();
        assertEquals(1, selectResult.size());

        TarantoolTuple tuple = selectResult.get(0);
        assertEquals(5, tuple.size());
        assertEquals(1, tuple.getInteger(0));
        assertNotNull(tuple.getInteger(1)); //bucket_id
        assertEquals("FIO", tuple.getString(2));
        assertEquals(50, tuple.getInteger(3));
        assertEquals(100, tuple.getInteger(4));

        // with fields
        SelectOptions options = ProxySelectOptions.create().withFields(Arrays.asList("profile_id", "age"));
        selectResult = profileSpace.select(conditions, options).get();
        assertEquals(1, selectResult.size());

        tuple = selectResult.get(0);
        assertEquals(2, tuple.size());
        assertEquals(1, tuple.getInteger(0));
        assertEquals(1, tuple.getInteger("profile_id"));
        assertEquals(50, tuple.getInteger("age"));
    }

    @Test
    public void withModeTest() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> operations =
            client.space(TEST_SPACE_NAME);

        operations.select(Conditions.any()).get();
        List<?> crudSelectOpts = client.eval("return crud_select_opts").get();
        assertNull(((HashMap<?, ?>) crudSelectOpts.get(0)).get("mode"));

        int customTimeout = 2000;
        operations.select(Conditions.any(), ProxySelectOptions.create().withTimeout(customTimeout)).get();
        crudSelectOpts = client.eval("return crud_select_opts").get();
        assertNull(((HashMap<?, ?>) crudSelectOpts.get(0)).get("mode"));

        operations.select(Conditions.any(), ProxySelectOptions.create().withMode(Mode.WRITE)).get();
        crudSelectOpts = client.eval("return crud_select_opts").get();
        assertEquals(Mode.WRITE.value(), ((HashMap<?, ?>) crudSelectOpts.get(0)).get("mode"));
    }

    @Test
    public void withFirstTest() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> operations =
            client.space(TEST_SPACE_NAME);

        //default
        operations.select(Conditions.any()).get();
        List<?> crudSelectOpts = client.eval("return crud_select_opts").get();
        assertEquals(4294967295L, ((HashMap<?, ?>) crudSelectOpts.get(0)).get("first"));

        operations.select(Conditions.limit(0)).get();
        crudSelectOpts = client.eval("return crud_select_opts").get();
        assertEquals(0, ((HashMap<?, ?>) crudSelectOpts.get(0)).get("first"));

        operations.select(Conditions.any(), ProxySelectOptions.create().withFirst(2)).get();
        crudSelectOpts = client.eval("return crud_select_opts").get();
        assertEquals(2, ((HashMap<?, ?>) crudSelectOpts.get(0)).get("first"));

        operations.select(Conditions.any(), ProxySelectOptions.create().withFirst(0)).get();
        crudSelectOpts = client.eval("return crud_select_opts").get();
        assertEquals(0, ((HashMap<?, ?>) crudSelectOpts.get(0)).get("first"));

        operations.select(Conditions.any(), ProxySelectOptions.create().withFirst(1)).get();
        crudSelectOpts = client.eval("return crud_select_opts").get();
        assertEquals(1, ((HashMap<?, ?>) crudSelectOpts.get(0)).get("first"));

        operations.select(Conditions.limit(3), ProxySelectOptions.create().withFirst(1)).get();
        crudSelectOpts = client.eval("return crud_select_opts").get();
        assertEquals(1, ((HashMap<?, ?>) crudSelectOpts.get(0)).get("first"));

        operations.select(Conditions.limit(1), ProxySelectOptions.create().withFirst(2)).get();
        crudSelectOpts = client.eval("return crud_select_opts").get();
        assertEquals(1, ((HashMap<?, ?>) crudSelectOpts.get(0)).get("first"));
    }

    @Test
    public void withAfterTest() throws ExecutionException, InterruptedException {
        TarantoolSpaceOperations<TarantoolTuple, TarantoolResult<TarantoolTuple>> operations =
            client.space(TEST_SPACE_NAME);
        TarantoolTuple firstTuple = new DefaultTarantoolTupleFactory(mapperFactory.defaultComplexTypesMapper()).create(
            Arrays.asList(1, 1, "kolya", 500));
        TarantoolTuple secondTuple = new DefaultTarantoolTupleFactory(mapperFactory.defaultComplexTypesMapper()).create(
            Arrays.asList(2, 1, "roma", 600));

        operations.select(Conditions.any()).get();
        List<?> crudSelectOpts = client.eval("return crud_select_opts").get();
        assertEquals(null, ((HashMap<?, ?>) crudSelectOpts.get(0)).get("after"));

        operations.select(Conditions.after(firstTuple)).get();
        crudSelectOpts = client.eval("return crud_select_opts").get();
        assertEquals(Arrays.asList(1, 1, "kolya", 500), ((HashMap<?, ?>) crudSelectOpts.get(0)).get("after"));

        operations.select(Conditions.any(), ProxySelectOptions.create().withAfter(firstTuple)).get();
        crudSelectOpts = client.eval("return crud_select_opts").get();
        assertEquals(Arrays.asList(1, 1, "kolya", 500), ((HashMap<?, ?>) crudSelectOpts.get(0)).get("after"));

        operations.select(Conditions.after(secondTuple), ProxySelectOptions.create().withAfter(firstTuple)).get();
        crudSelectOpts = client.eval("return crud_select_opts").get();
        assertEquals(Arrays.asList(2, 1, "roma", 600), ((HashMap<?, ?>) crudSelectOpts.get(0)).get("after"));
    }
}
