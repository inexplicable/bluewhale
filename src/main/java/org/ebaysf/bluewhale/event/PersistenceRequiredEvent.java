package org.ebaysf.bluewhale.event;

import java.util.EventObject;

/**
 * Created by huzhou on 3/7/14.
 */
public class PersistenceRequiredEvent extends EventObject {

    public PersistenceRequiredEvent(Object source) {

        super(source);
    }
}
