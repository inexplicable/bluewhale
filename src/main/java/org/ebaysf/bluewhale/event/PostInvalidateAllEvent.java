package org.ebaysf.bluewhale.event;

import org.ebaysf.bluewhale.segment.Segment;
import org.ebaysf.bluewhale.storage.UsageTrack;

import java.util.Collection;
import java.util.EventObject;

/**
 * Created by huzhou on 3/2/14.
 */
public class PostInvalidateAllEvent extends EventObject {

    private final UsageTrack _fromScratch;

    public PostInvalidateAllEvent(final Collection<Segment> abandoned, final UsageTrack fromScratch) {

        super(abandoned);

        _fromScratch = fromScratch;
    }

    public @Override Collection<Segment> getSource(){
        return (Collection<Segment>)super.getSource();
    }

    public UsageTrack getUsageTrack(){
        return (UsageTrack)_fromScratch;
    }
}
