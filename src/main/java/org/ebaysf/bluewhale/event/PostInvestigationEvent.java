package org.ebaysf.bluewhale.event;

import com.google.common.collect.ListMultimap;
import org.ebaysf.bluewhale.storage.BinJournal;
import org.ebaysf.bluewhale.storage.BinStorage;

import java.util.EventObject;

/**
 * Created by huzhou on 3/5/14.
 */
public class PostInvestigationEvent extends EventObject {

    public PostInvestigationEvent(final ListMultimap<BinStorage.InspectionReport, BinJournal> source) {

        super(source);
    }

    public @Override ListMultimap<BinStorage.InspectionReport, BinJournal> getSource(){
        return (ListMultimap<BinStorage.InspectionReport, BinJournal>)super.getSource();
    }
}
