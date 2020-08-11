package io.tarantool.driver.providers;

import io.tarantool.driver.ServerAddress;
import io.tarantool.driver.cluster.RoundRobinAddressProvider;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RoundRobinAddressProviderTest {

    @Test
    public void getAddress() {
        List<ServerAddress> addressList = Arrays.asList(
                new ServerAddress("127.0.0.1", 3301),
                new ServerAddress("127.0.0.2", 3302),
                new ServerAddress("127.0.0.3", 3303),
                new ServerAddress("127.0.0.1", 3301) //duplicate
        );

        RoundRobinAddressProvider provider = new RoundRobinAddressProvider(addressList);

        assertEquals(3, provider.size());
        assertEquals("127.0.0.1", provider.getNext().getHost());
        assertEquals("127.0.0.2", provider.getNext().getHost());
        assertEquals("127.0.0.3", provider.getNext().getHost());

        assertEquals("127.0.0.1", provider.getNext().getHost());
        assertEquals("127.0.0.2", provider.getNext().getHost());
        assertEquals("127.0.0.2", provider.getAddress().getHost());
        assertEquals(3302, provider.getAddress().getPort());
        assertEquals("127.0.0.3", provider.getNext().getHost());

        assertEquals("127.0.0.1", provider.getNext().getHost());
    }

    @Test
    public void updateAddressList() {
        List<ServerAddress> addressList = Arrays.asList(
                new ServerAddress("127.0.0.1", 3301),
                new ServerAddress("127.0.0.2", 3301),
                new ServerAddress("127.0.0.3", 3301)
        );

        RoundRobinAddressProvider provider = new RoundRobinAddressProvider(addressList);

        assertEquals(3, provider.size());
        assertEquals("127.0.0.1", provider.getNext().getHost());
        assertEquals("127.0.0.2", provider.getNext().getHost());
        assertEquals("127.0.0.3", provider.getNext().getHost());

        List<ServerAddress> newAddressList = Arrays.asList(
                new ServerAddress("10.0.2.10", 3301),
                new ServerAddress("10.0.2.11", 3301),
                new ServerAddress("127.0.0.3", 3301), //current address
                new ServerAddress("10.0.2.12", 3301),
                new ServerAddress("10.0.2.13", 3301)
        );

        provider.updateAddressList(newAddressList);

        assertEquals(5, provider.size());
        assertEquals("127.0.0.3", provider.getAddress().getHost());
        assertEquals("10.0.2.12", provider.getNext().getHost());
        assertEquals("10.0.2.13", provider.getNext().getHost());
        assertEquals("10.0.2.10", provider.getNext().getHost());
    }
}