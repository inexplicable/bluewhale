package org.ebaysf.bluewhale.event;

import org.ebaysf.bluewhale.storage.BinStorage;

import java.util.EventObject;

/**
 * Created by huzhou on 3/8/14.
 */
public class RequestInvestigationEvent extends EventObject {

    public RequestInvestigationEvent(final BinStorage storage){

        super(storage);
    }

    public @Override BinStorage getSource(){

        return (BinStorage)super.getSource();
    }
}
