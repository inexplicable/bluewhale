package org.ebaysf.bluewhale.event;

import org.ebaysf.bluewhale.storage.BinJournal;

import java.util.EventObject;

/**
 * Created by huzhou on 3/3/14.
 */
public class DocumentLengthAnticipatedEvent extends EventObject {

    private final int _documentLengthAnticipated;

    public DocumentLengthAnticipatedEvent(final BinJournal source,
                                          final int documentLengthAnticipated){

        super(source);
        _documentLengthAnticipated = documentLengthAnticipated;
    }

    public @Override BinJournal getSource(){

        return (BinJournal)super.getSource();
    }

    public int getDocumentLengthAnticipated(){

        return _documentLengthAnticipated;
    }
}
