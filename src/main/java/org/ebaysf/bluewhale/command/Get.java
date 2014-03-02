package org.ebaysf.bluewhale.command;

import java.util.concurrent.Callable;

/**
 * Created by huzhou on 2/26/14.
 */
public interface Get {

    Object getKey();

    int getHashCode();

    boolean loadIfAbsent();

    <V> Callable<V> getValueLoader();

}
