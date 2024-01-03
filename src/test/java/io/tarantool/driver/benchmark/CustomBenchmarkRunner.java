package io.tarantool.driver.benchmark;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tarantool.driver.api.TarantoolClient;
import io.tarantool.driver.api.TarantoolClientFactory;
import io.tarantool.driver.api.TarantoolResult;
import io.tarantool.driver.api.tuple.TarantoolTuple;

public class CustomBenchmarkRunner {
    final static Logger log = LoggerFactory.getLogger(CustomBenchmarkRunner.class);
    public static final String ECHO_EXPRESSION = "return ...";
    public static final String YEAR = "2022";
    public static final String MONTH = "07";
    public static final String DAY = "01";
    public static final String HOUR = "08";
    public static final String MINUTES = "30";
    public static final String SECONDS = "05";
    public static final String NANOSECONDS = "000000123";
    public static final String T_2022_08_30_05 =
        String.format("%s-%s-%sT%s:%s:%s", YEAR, MONTH, DAY, HOUR, MINUTES, SECONDS);
    public static final String T_2022_08_30_05_000000123 = String.format("%s.%s", T_2022_08_30_05, NANOSECONDS);
    public static final LocalDateTime LOCAL_DATE_TIME = LocalDateTime.parse(T_2022_08_30_05_000000123);
    public static final long EPOCH_SECOND = LOCAL_DATE_TIME.toEpochSecond(ZoneOffset.UTC);
    public static final List<Object> ECHO_ARGS = Arrays.asList(
        "string",
        Long.MAX_VALUE,
        Integer.MAX_VALUE,
        Short.MAX_VALUE,
        Double.MAX_VALUE,
        Float.MAX_VALUE,
        true,
        false,
//        null,                                                // it doesn't work in cartridge-java
//        new Interval().setYear(1).setMonth(200).setDay(-77), // it doesn't work in cartridge-java
        new ArrayList<>(Arrays.asList(1, "a", null, true, new HashMap<Object, Object>() {{
            put("nestedArray", new ArrayList<>(Arrays.asList(1, "a", null, true)));
        }})),
        new HashMap<Object, Object>() {{
            put("a", 1);
            put("b", "3");
            put("99", true);
            put("nestedArray", new ArrayList<>(Arrays.asList(1, "a", null, true, new HashMap<Object, Object>() {{
                put("hello", "world");
            }})));
        }},
        new BigDecimal("9223372036854775808"),
        UUID.randomUUID(),
        Instant.ofEpochSecond(EPOCH_SECOND)
    );

    public static final long NANOS_PER_SECOND = 1000_000_000L;
    private static long lastRps = 0;

    private static TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> getClient(Integer connections) {
        log.debug("Attempting connect to Tarantool");
        TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = TarantoolClientFactory.createClient()
            .withAddress(System.getenv().getOrDefault("TARANTOOL_HOST", "localhost"), 3301)
            .withConnections(connections)
            .withConnectTimeout(3000)
            .build();

        log.debug("Successfully connected to Tarantool, version = {}", client.getVersion());
        return client;
    }

    public static void main(String[] args) throws Exception {
        List<Integer> nanosBetweenSending = new ArrayList<>();
        for (int rps = 10_000; rps <= 1_000_000; rps += 10_000) {
            nanosBetweenSending.add(getDelay(rps));
        }

        List<Integer> connectionsAmount = Arrays.asList(
            1
        );
        Integer durationInSeconds = 20;
        List<BiFunction<TarantoolClient, Integer, Boolean>> tests = Arrays.asList(
            (client, nanos) -> {
                try {
                    return simpleCall(client, nanos, durationInSeconds);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        );
        for (BiFunction<TarantoolClient, Integer, Boolean> test : tests) {
            for (Integer connections : connectionsAmount) {
                TarantoolClient<TarantoolTuple, TarantoolResult<TarantoolTuple>> client = getClient(connections);
                for (Integer nanos : nanosBetweenSending) {
                    Boolean errorHappened = test.apply(client, nanos);
                    if (errorHappened) {
                        break;
                    }
                }
                client.close();
                System.gc();
                Thread.sleep(5000);
            }
        }
    }

    private static int getDelay(int rps) {
        return (int) (NANOS_PER_SECOND / rps);
    }


    public static Boolean simpleCall(TarantoolClient client, Integer delay, Integer duration) throws Exception {
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        return runRequestsWithDelay(
            methodName, client, delay, duration,
            () -> client.eval(ECHO_EXPRESSION, ECHO_ARGS)
        );
    }

    private static boolean runRequestsWithDelay(
        String methodName,
        TarantoolClient client, Integer delay, Integer duration, Supplier<CompletableFuture> supplier)
    throws InterruptedException {

        AtomicInteger counter = new AtomicInteger();
        AtomicBoolean errorHappened = new AtomicBoolean(false);

        long start = Instant.now().getEpochSecond();
        Thread thread = startClientThread(delay, supplier, counter, errorHappened);

        return controlLoop(client, delay, duration, methodName, counter, errorHappened, start, thread);
    }

    @NotNull
    private static Thread startClientThread(
        Integer delay, Supplier<CompletableFuture> supplier, AtomicInteger counter, AtomicBoolean errorHappened) {
        Thread thread = new Thread(() -> {
            while (true) {
                if (errorHappened.get()) {
                    break;
                }
                long dstart = System.nanoTime();
                supplier.get()
                    .whenComplete((r, ex) -> {
                        if (ex != null) {
                            log.info(ex.toString());
                            errorHappened.set(true);
                        }
                        counter.getAndIncrement();
                    });
                if (Thread.currentThread().isInterrupted()) {
                    break;
                }
                waitDelay(dstart, delay);
            }
        });
        thread.setName("client_thread");
        thread.start();
        return thread;
    }

    private static boolean controlLoop(
        TarantoolClient client, Integer delay, Integer duration, String methodName, AtomicInteger counter,
        AtomicBoolean errorHappened, long start, Thread thread) throws InterruptedException {
        while (true) {
            if (errorHappened.get()) {
                log.info("error - {}(connections = {}, delay = {}, duration = {})",
                    methodName, client.getConfig().getConnections(), delay, duration);
                thread.interrupt();
                return true;
            }
            Thread.sleep(2000);
            long gap = Instant.now().getEpochSecond() - start;
            if (gap != 0) {
                long rps = counter.get() / gap;
                log.debug("{}, intermediate RPS: {}", methodName, rps);
                if (errorHappened.get() || counter.get() == 0) {
                    log.info("error - {}(connections = {}, delay = {}, duration = {})",
                        methodName, client.getConfig().getConnections(), delay, duration);
                    thread.interrupt();
                    return true;
                }
                if (gap > duration) {
                    log.info("success - {}(connections = {}, delay = {}, duration = {}) " +
                            ", RPS: {}",
                        methodName, client.getConfig().getConnections(), delay, duration, rps);
                    thread.interrupt();
                    if (lastRps > rps) {
                        lastRps = 0;
                        return true;
                    }
                    lastRps = rps;
                    return false;
                }
            }
        }
    }

    private static void waitDelay(Long dstart, Integer delay) {
        while (dstart + delay >= System.nanoTime()) ;
    }
}
