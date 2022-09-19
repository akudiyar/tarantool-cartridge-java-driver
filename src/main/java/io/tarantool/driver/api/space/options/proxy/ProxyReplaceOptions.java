package io.tarantool.driver.api.space.options.proxy;

import io.tarantool.driver.api.space.options.ReplaceOptions;

/**
 * Represent options for replace cluster proxy operation
 *
 * @author Alexey Kuzin
 * @author Artyom Dubinin
 */
public final class ProxyReplaceOptions extends ProxyBaseOptions<ProxyReplaceOptions> implements ReplaceOptions {

    private ProxyReplaceOptions() {
    }

    public static ProxyReplaceOptions create() {
        return new ProxyReplaceOptions();
    }

    @Override
    protected ProxyReplaceOptions self() {
        return this;
    }
}