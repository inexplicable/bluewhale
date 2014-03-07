package org.ebaysf.bluewhale.event;

import org.ebaysf.bluewhale.segment.Segment;

/**
 * Created by huzhou on 3/1/14.
 */
public class PostSegmentSplitEvent extends PersistenceRequiredEvent {

    public PostSegmentSplitEvent(final Object source) {

        super(source);
    }

    public @Override Segment getSource(){

        return (Segment)super.getSource();
    }
}
