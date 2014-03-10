package org.ebaysf.bluewhale.configurable;

import org.ebaysf.bluewhale.command.PutAsRefresh;
import org.ebaysf.bluewhale.document.BinDocument;
import org.ebaysf.bluewhale.segment.Segment;
import org.ebaysf.bluewhale.storage.BinStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by huzhou on 3/6/14.
 */
public interface EvictionStrategy {

    Logger LOG = LoggerFactory.getLogger(EvictionStrategy.class);

    EvictionStrategy SILENCE = new EvictionStrategy() {

        public @Override void afterGet(final Segment segment,
                                       final BinStorage storage,
                                       final long token,
                                       final BinDocument doc) {

        }

        public @Override void afterPut(final Segment segment,
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
                    LOG.error("LRU refresh failed", e);
                }
            }
        }

        public @Override void afterPut(final Segment segment,
                                       final BinStorage storage,
                                       final long token,
                                       final BinDocument doc) {

        }
    };

    void afterGet(final Segment segment,
                  final BinStorage storage,
                  final long token,
                  final BinDocument doc);

    void afterPut(final Segment segment,
                  final BinStorage storage,
                  final long token,
                  final BinDocument doc);

}
