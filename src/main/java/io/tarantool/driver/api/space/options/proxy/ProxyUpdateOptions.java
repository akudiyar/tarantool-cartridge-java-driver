package io.tarantool.driver.api.space.options.proxy;

import io.tarantool.driver.api.space.options.UpdateOptions;

/**
 * Represent options for update cluster proxy operation
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public final class ProxyUpdateOptions extends ProxyBaseOptions<ProxyUpdateOptions> implements UpdateOptions {

    private ProxyUpdateOptions() {
    }

    public static ProxyUpdateOptions create() {
        return new ProxyUpdateOptions();
    }

    @Override
    protected ProxyUpdateOptions self() {
        return this;
    }
}