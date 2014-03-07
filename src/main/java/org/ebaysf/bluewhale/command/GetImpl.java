package org.ebaysf.bluewhale.command;

import com.google.common.cache.AbstractCache;

import java.util.concurrent.Callable;

/**
 * Created with IntelliJ IDEA.
 * User: huzhou
 * Date: 12/12/12
 * Time: 12:21 AM
 * To change this template use File | Settings | File Templates.
 */
public class GetImpl implements Get {

    private final Object _key;
    private final Callable<?> _valueLoader;
    private final int _hashCode;
    private final boolean _loadIfAbsent;
    private final AbstractCache.StatsCounter _statsCounter;

    public <V> GetImpl(final Object key,
                   final Callable<V> valueLoader,
                   final int hashCode,
                   final boolean loadIfAbsent,
                   final AbstractCache.StatsCounter statsCounter) {

        _key = key;
        _valueLoader = valueLoader;
        _hashCode = hashCode;
        _loadIfAbsent = loadIfAbsent;
        _statsCounter = statsCounter;
    }

    public @Override Object getKey() {
        return _key;
    }

    public @Override <V> Callable<V> getValueLoader() {
        return (Callable<V>)_valueLoader;
    }

    public @Override int getHashCode() {
        return _hashCode;
    }

    public @Override boolean loadIfAbsent(){
        return _loadIfAbsent;
    }

    public @Override AbstractCache.StatsCounter getStatsCounter(){
        return _statsCounter;
    }

}
