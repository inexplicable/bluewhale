package org.ebaysf.bluewhale.event;

import org.ebaysf.bluewhale.storage.BinJournal;
import org.ebaysf.bluewhale.storage.BinStorage;

import java.util.EventObject;

/**
 * Created by huzhou on 3/1/14.
 */
public class PostExpansionEvent extends EventObject {

    private final BinJournal _previous;

    public PostExpansionEvent(final BinStorage source, final BinJournal previous) {

        super(source);

        _previous = previous;
    }

    public @Override BinStorage getSource() {

        return (BinStorage)super.getSource();
    }

    public BinJournal getPreviousWritable(){

        return _previous;
    }
}
