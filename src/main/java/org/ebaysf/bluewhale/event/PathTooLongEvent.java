package org.ebaysf.bluewhale.event;

import org.ebaysf.bluewhale.segment.Segment;

import java.util.EventObject;

/**
 * Created by huzhou on 3/4/14.
 */
public class PathTooLongEvent extends EventObject {

    private final int _offset;
    private final long _token;

    public PathTooLongEvent(final Segment segment,
                            final int offset,
                            final long headToken) {

        super(segment);

        _offset = offset;
        _token = headToken;
    }

    public @Override Segment getSource() {

        return (Segment)super.getSource();
    }

    public int getOffset() {

        return _offset;
    }

    public long getHeadToken() {

        return _token;
    }
}
