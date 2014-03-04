package org.ebaysf.bluewhale.event;

import com.google.common.cache.RemovalCause;
import org.ebaysf.bluewhale.document.BinDocument;

import java.util.EventObject;

/**
 * Created by huzhou on 3/3/14.
 */
public class RemovalNotificationEvent extends EventObject {

    private final RemovalCause _cause;

    public RemovalNotificationEvent(final BinDocument source,
                                    final RemovalCause cause) {

        super(source);

        _cause = cause;
    }

    public @Override BinDocument getSource() {

        return (BinDocument)super.getSource();
    }

    public RemovalCause getRemovalCase(){

        return _cause;
    }
}
