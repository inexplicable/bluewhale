package org.ebaysf.bluewhale.event;

import com.google.common.collect.Lists;
import org.ebaysf.bluewhale.segment.Segment;

import java.util.EventObject;
import java.util.List;

/**
 * Created by huzhou on 2/26/14.
 */
public class SegmentSplitEvent extends EventObject {

    private final List<Segment> _splitted;

    public SegmentSplitEvent(final Segment source, final List<Segment> splitted) {
        super(source);

        _splitted = Lists.newArrayList(splitted);
    }

    public Segment before(){

        return (Segment) getSource();
    }

    public List<Segment> after(){

        return _splitted;
    }
}
