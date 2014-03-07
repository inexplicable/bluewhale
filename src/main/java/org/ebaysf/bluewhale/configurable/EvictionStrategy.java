package org.ebaysf.bluewhale.configurable;

import com.google.common.base.Throwables;
import org.ebaysf.bluewhale.command.PutAsRefresh;
import org.ebaysf.bluewhale.document.BinDocument;
import org.ebaysf.bluewhale.segment.Segment;
import org.ebaysf.bluewhale.storage.BinStorage;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * Created by huzhou on 3/6/14.
 */
public interface EvictionStrategy {

    Logger LOG = Logger.getLogger(EvictionStrategy.class.getName());

    EvictionStrategy SILENCE = new EvictionStrategy() {

        public @Override void afterGet(final Segment segment,
                                       final BinStorage storage,
                                       final long token,
                                       final BinDocument doc) {

        }
    };

    EvictionStrategy LRU = new EvictionStrategy() {

        public @Override void afterGet(final Segment segment,
                                       final BinStorage storage,
                                       final long token,
                                       final BinDocument doc) {

            if(!doc.isTombstone() && storage.isDangerous(token)){
                try {
                    segment.put(new PutAsRefresh(doc.getKey(), doc.getValue(), doc.getHashCode(), doc.getState()));
                }
                catch (IOException e) {
                    LOG.warning(Throwables.getStackTraceAsString(e));
                }
            }
        }
    };

    void afterGet(final Segment segment,
                  final BinStorage storage,
                  final long token,
                  final BinDocument doc);

}
