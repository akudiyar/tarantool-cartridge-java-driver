package io.tarantool.driver.api.client.parameterwrapper;

import io.tarantool.driver.auth.TarantoolCredentials;

public class TarantoolCredentialsWrapper implements TarantoolClientParameter<TarantoolCredentials> {

    private final TarantoolCredentials credentials;

    public TarantoolCredentialsWrapper(TarantoolCredentials credentials) {
        this.credentials = credentials;
    }

    public TarantoolCredentials getValue() {
        return this.credentials;
    }
}
