package org.ebaysf.bluewhale.util;

import com.google.common.base.Equivalence;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.MapMaker;

import java.lang.reflect.Method;
import java.util.Map;

/**
 * MapMaker doesn't expose its keyEquivalence api
 */
public final class Maps {

    private final Method _keyEquivalenceMethodOfMapMaker;
    private final Method _keyEquivalenceMethodOfCacheBuilder;

    public static final Maps INSTANCE = new Maps();
    
    private Maps(){

        try{
            _keyEquivalenceMethodOfMapMaker = MapMaker.class.getDeclaredMethod("keyEquivalence", Equivalence.class);
            _keyEquivalenceMethodOfMapMaker.setAccessible(true);
            
            _keyEquivalenceMethodOfCacheBuilder = CacheBuilder.class.getDeclaredMethod("keyEquivalence", Equivalence.class);
            _keyEquivalenceMethodOfCacheBuilder.setAccessible(true);
        }
        catch(Exception e){
            throw new IllegalStateException(e);
        }
    }
    
    public <K, V> Map<K, V> newIdentityMap(){

        MapMaker mapMaker = new MapMaker();
        try{
            mapMaker = (MapMaker)_keyEquivalenceMethodOfMapMaker.invoke(mapMaker, Equivalence.identity());
        }
        catch(Exception e){
            throw new IllegalStateException(e);
        }

        return mapMaker.makeMap();
    }
    
    @SuppressWarnings("unchecked")
	public <K, V> Cache<K, V> newIdentityWeakValuesCache(final RemovalListener<K, V> removalListener){
    	CacheBuilder<K, V> cacheBldr = CacheBuilder.newBuilder()
    				.weakValues()
    				.removalListener(removalListener);
    	
    	try{
    		cacheBldr = (CacheBuilder<K, V>)_keyEquivalenceMethodOfCacheBuilder.invoke(cacheBldr, Equivalence.identity());
        }
        catch(Exception e){
            throw new IllegalStateException(e);
        }
    	
    	return cacheBldr.build();
    }
}
