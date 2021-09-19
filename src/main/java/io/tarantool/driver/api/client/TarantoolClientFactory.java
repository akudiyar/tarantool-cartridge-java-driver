package io.tarantool.driver.api.client;

public interface TarantoolClientFactory {

    static TarantoolClientBuilder createClient() {
        return new TarantoolClientBuilderImpl();
    }

}
