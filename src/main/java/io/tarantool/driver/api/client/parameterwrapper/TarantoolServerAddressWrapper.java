package io.tarantool.driver.api.client.parameterwrapper;

import io.tarantool.driver.TarantoolServerAddress;

import java.util.List;

public class TarantoolServerAddressWrapper implements TarantoolClientParameter {

    public TarantoolServerAddressWrapper(List<TarantoolServerAddress> addressList) {
        this.addressList = addressList;
    }

    private final List<TarantoolServerAddress> addressList;

    public List<TarantoolServerAddress> getValue() {
        return addressList;
    }
}
