package org.ebaysf.bluewhale.segment;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.cache.RemovalCause;
import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.Subscribe;
import org.ebaysf.bluewhale.Cache;
import org.ebaysf.bluewhale.command.Get;
import org.ebaysf.bluewhale.command.Put;
import org.ebaysf.bluewhale.command.PutAsIs;
import org.ebaysf.bluewhale.document.BinDocument;
import org.ebaysf.bluewhale.event.PostInvalidateAllEvent;
import org.ebaysf.bluewhale.event.PostSegmentSplitEvent;
import org.ebaysf.bluewhale.event.RemovalNotificationEvent;
import org.ebaysf.bluewhale.event.SegmentSplitEvent;
import org.ebaysf.bluewhale.serialization.Serializer;
import org.ebaysf.bluewhale.storage.BinStorage;
import org.javatuples.Pair;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by huzhou on 2/28/14.
 */
public class LeafSegment extends AbstractSegment {

    private final ByteBuffer _mmap;
    private final LongBuffer _tokens;

    public LeafSegment(final Range<Integer> range,
                       final Cache<?, ?> belongsTo,
                       final SegmentsManager manager,
                       final ByteBuffer mmap) {

        super(range, belongsTo, manager);

        _mmap = mmap;
        _tokens = _mmap.asLongBuffer();

        belongsTo().getEventBus().register(this);
        _manager.rememberBufferUsedBySegment(_mmap, this);
    }

    @Override
    public <V> V get(Get get) throws ExecutionException, IOException {
        //promised! no locking on Get
        if(isLeaf()){
            final int offset = getOffset(get.getHashCode());
            final long token = _tokens.get(offset);

            final V hit = getIfPresent(offset, token, get);
            if(hit != null){
                return hit;
            }
            if(!get.loadIfAbsent() || get.getValueLoader() == null){
                return null;
            }

            final long beforeLoad = System.nanoTime();
            final Future<V> value = belongsTo().getExecutor().submit(get.<V>getValueLoader());
            try{

                final V resolved = value.get();
                System.out.printf("resolved: %s\n", resolved);

                put(new PutAsIs(get.getKey(), resolved, get.getHashCode()));
                //we do the blocking put, and then try calling get again
                return resolved;
            }
            catch(InterruptedException e){
                throw new ExecutionException(e);//value get failure
            }
        }
        /*
            this is the interesting idea needs some explanation.
            1st of all, remember that the concurrency guarantee is that: non-blocking read and consistent writes
            therefore when multi threads do readings, they might all find the a segment which is about to be splitted
            and the ordering is not mandated, which means that the read might see itself as leaf after a split has been done.
            therefore, when it fails to find the hashed slot after the split, it's possible that it's already splitted and we just try again
            using super#get which will check whether further routing must be done.
            2nd thing, why we allowed this is mainly for the maximized parallelism. when split happens, we don't hang any of our reads.
            and despite the order of concurrent reads and split, the read will successfully find the slot.
            moreover, this temporary extra routing is to be romoved which we refer to as "rooting" in an asynchronous task.
            the new leafs will climb up to the be direct children of the root later, in order to save the memory cost by such relay branch nodes.
            same is done in case of PUT
        */
        return super.get(get);
    }

    public @Override void put(Put put) throws IOException {
        try{
            _lock.lock();
            if(isLeaf()){
                final int offset = getOffset(put.getHashCode());
                final long next = _tokens.get(offset);
                if(isPutObsolete(next, put)){
                    return;//this is because of the background optimization tasks
                }

                final BinStorage bin = getStorage();
                final long token = bin.append(put.create(getKeySerializer(), getValSerializer(), next));
                _tokens.put(offset, token);

                //we must check the size change here, then trigger possible splits
                if(isSplitRequired(next, put)){
                    split();
                }
                else if(!put.resets() && next >= 0){//we won't do path shortening till read spotted the long paths
                    notifyRemoval(put, next);
                }
                return;//must return here, otherwise it goes into infinite recursion.
            }
        }
        finally{
            _lock.unlock();
        }
        super.put(put);
    }

    public @Override boolean using(final BinDocument suspect) {

        if(isLeaf()){
            //uses is much like get, in terms of non-blocking nature, even if the segment gets splitted async
            final long token = _tokens.get(getOffset(suspect.getHashCode()));
            try {
                return token >= 0 && using(suspect.getKey(), token, suspect.getLastModified());
            }
            catch (IOException e) {
                e.printStackTrace();
                //dangerous, conservative
                return true;
            }
        }
        return super.using(suspect);
    }

    public @Override void evict(final BinDocument obsolete, final RemovalCause cause) {

        if(isLeaf()){

            _belongsTo.getEventBus().post(new RemovalNotificationEvent(obsolete, cause));
        }
        else{
            super.evict(obsolete, cause);
        }
    }

    protected boolean using(final ByteBuffer keyAsBytes, final long token, final long lastModified) throws IOException {

        final BinStorage storage = getStorage();
        final Serializer keySerializer = getKeySerializer();

        //check each doc in the path, including the suspect itself, if there's any element of equivalent key
        //compare their last modified time, if there's newer document than suspect, return using=false, otherwise true
        for(BinDocument doc = storage.read(token); doc != null; doc = storage.read(doc.getNext())){
            if(keySerializer.equals(keyAsBytes, doc.getKey())){
                return lastModified >= doc.getLastModified();
            }
        }
        //shouldn't ever happen, because the document should be compared with itself in the loop above
        return false;
    }

    protected <V> V getIfPresent(final int offset, final long token, final Get get) throws IOException{

        final BinStorage storage = getStorage();
        final Serializer keySerializer = getKeySerializer();
        final Object key = get.getKey();

        int length = 0;
        try{
            for(BinDocument doc = storage.read(token); doc != null; doc = storage.read(doc.getNext()), length += 1){

                if(keySerializer.equals(key, doc.getKey())){

                    return (V) (doc.isTombstone() ? null : getValSerializer().deserialize(doc.getValue(), doc.isCompressed()));
                }
            }
            return null;
        }
        finally {

            //TODO shorten path
        }
    }

    /**
     * <p>
     * NOTE, as we're going to do more async tasks, like path shortening, async value loadings etc. we'll need to accept
     * remedy PUTs which might be in fact obsolete as some other updates were written, the only way to prevent that is
     * to provide modified timestamp to compare.
     *
     * Let's see an example here, where a PUT is done, a Path shortening is issued, another thread does another PUT
     * The consistent result would be that the Path shortening results in a PUT which is older than the 2nd PUT
     * And therefore, we must filter that out. On the other hand, if it's the other way around, meaning that the 2nd PUT
     * doesn't happen till the Path shortening is done, both of the PUTs will just happen without causing any problem.
     *
     * Another example could be with value loading, though yet supported at this moment. The idea is to speed up Get with value loading,
     * when multiple threads getting a missing value, all will enter the lock region, only one would be allowed to do the actual Put.
     * And all the rest would be allowed to enter later, and each will see the token value changed and forced to retry the Get entirely.
     * As explained above, the performance bottleneck esp. with respect to parallelism is the fact that all threads will be pending on the
     * one who does the Put. But that's dependant on how fast the value loading could be one. Initially we use busy wait on Future here.
     * Obviously this isn't the most optimal. An alternative is to Put a value loading document, with value part only indicating that it's a future.
     * Then the Put will complete as fast as possible, and each other thread will be allowed to see the Future too. Then we use the busy wait to
     * actually retrieve the value. At least, the bottleneck would be shifted out of the cache structures, and more parallelism achieved.
     * Well, this isn't the end of it. For one, we must hold the value loading future somehow to allow it to be seen by all threads temporarily.
     * Next, as we say temporarily, the value loading would finish later, and an actual value would now be a faster resort. We need to do a value resolved
     * task to update with the loaded value. And that's when we need again, the last modified time. As the loaded value shouldn't use now, but rather
     * the previously value loading's last modified time so as not to overwrite any later changes.
     *
     * </p>
     * @param next
     * @param put
     * @return
     * @throws IOException
     */
    protected boolean isPutObsolete(final long next, final Put put) throws IOException{
        final BinStorage bin = getStorage();
        //verify if reset is ok
        if(put.resets()){
            return next != put.getHeadToken();
        }
        //verify normal updates is ok
        for(BinDocument doc = bin.read(next); doc != null; doc = bin.read(doc.getNext())) {

            if(getKeySerializer().equals(put.getKey(getKeySerializer()), doc.getKey())) {
                return put.getLastModified() < doc.getLastModified();//the PUT is created ahead of the existing docs, filter it out
            }
        }
        return false;
    }

    /**
     * happens at any Put command handlings, which calculates the size and verify if a split is needed.
     * @param next
     * @param put
     * @return
     * @throws IOException
     */
    protected boolean isSplitRequired(final long next, final Put put) throws IOException{
        //resets/invalidates doesn't increase the size!
        if(put.resets()
                || put.invalidates()
                || range().upperEndpoint() - range().lowerEndpoint() <= 1) {

            return false;
        }

        final BinStorage bin = getStorage();
        final Object key = put.getKey(getKeySerializer());
        for(BinDocument doc = bin.read(next); doc != null; doc = bin.read(doc.getNext())){

            if(getKeySerializer().equals(key, doc.getKey())){
                //overwrites a tombstone, size increments
                return doc.isTombstone() && (_size += 1) > MAX_TOKENS_IN_ONE_SEGMENT;
            }
        }

        //no previous actions upon this key
        return (_size += 1) > MAX_TOKENS_IN_ONE_SEGMENT;
    }

    protected void split() throws IOException {

        final int lowerBound = range().lowerEndpoint();
        final int upperBound = range().upperEndpoint();

        Preconditions.checkArgument(lowerBound != upperBound, "cannot further split!");

        final int splitAt = (int)(((long)lowerBound +(long)upperBound) >> 1L);
        final BinStorage bin = getStorage();

        final LeafSegment lower = newLeafSegment(_manager.allocateBuffer(), Range.closed(lowerBound, splitAt));
        final LeafSegment upper = newLeafSegment(_manager.allocateBuffer(), Range.closed(splitAt + 1, upperBound));

        for(int offset = 0, len = _tokens.capacity(); offset < len; offset += 1){
            final long token = _tokens.get(offset);
            if(token >= 0){
                final Map<Object, Pair<Long, BinDocument>> groupByKey = Maps.newLinkedHashMap();
                //1st, filter all docs, keeping only the 1st doc of each unique key
                long nextToken = token;
                for(BinDocument doc = bin.read(nextToken); doc != null; nextToken = doc.getNext(), doc = bin.read(nextToken)){

                    final Object key = getKeySerializer().deserialize(doc.getKey(), false);
                    if(!groupByKey.containsKey(key)){
                        groupByKey.put(key, new Pair<Long, BinDocument>(Long.valueOf(nextToken), doc));
                    }
                }

                //2nd, sweep the filtered documents map again, remove all tombstones, those keys were simply invalidated
                //3rd, actual split, including size calculations.
                boolean lowerHeadGiven = false, upperHeadGiven = false;
                for(Pair<Long, BinDocument> survived : Collections2.filter(groupByKey.values(), _nonTombstonePredicate)){
                    final long tokenSurvived = survived.getValue0().longValue();
                    final BinDocument docSurvived = survived.getValue1();
                    final int segmentCode = getSegmentCode(docSurvived.getHashCode());
                    if(segmentCode >= lower.range().lowerEndpoint() && segmentCode <= lower.range().upperEndpoint()){
                        if(!lowerHeadGiven){
                            lower._tokens.put(offset, tokenSurvived);
                            lowerHeadGiven = true;
                        }
                        lower._size += 1;
                    }
                    else if(segmentCode >= upper.range().lowerEndpoint() && segmentCode <= upper.range().upperEndpoint()){
                        if(!upperHeadGiven){
                            upper._tokens.put(offset, tokenSurvived);
                            upperHeadGiven = true;
                        }
                        upper._size += 1;
                    }
                }
            }
        }

        //i wish i have [_lower, _upper] = [lower, upper] in one unit of operation
        _lower = lower;
        _upper = upper;

        //this triggers tasks like RootingTask
        _belongsTo.getEventBus().post(new SegmentSplitEvent(this, getChildren()));
    }

    protected void notifyRemoval(final Put put, final long next) {
        if(!put.suppressRemovalNotification()){

            final Serializer<Object> keySerializer = getKeySerializer();
            final Object key = put.getKey(keySerializer);
            final BinStorage storage = getStorage();

            try {
                if(put.invalidates()){
                    for(BinDocument doc = storage.read(next); doc != null; doc = storage.read(doc.getNext())){
                        if(keySerializer.equals(key, doc.getKey())){
                            if(!doc.isTombstone()){
                                _belongsTo.getEventBus().post(new RemovalNotificationEvent(doc, RemovalCause.EXPLICIT));
                            }
                            return;
                        }
                    }
                }
                else{
                    for(BinDocument doc = storage.read(next); doc != null; doc = storage.read(doc.getNext())){
                        if(keySerializer.equals(key, doc.getKey())){
                            if(!doc.isTombstone()){
                                _belongsTo.getEventBus().post(new RemovalNotificationEvent(doc, RemovalCause.REPLACED));
                            }
                            return;
                        }
                    }
                }
            }
            catch (IOException e) {

                e.printStackTrace();
            }
        }
    }

    @Subscribe
    protected void onPostSegmentSplit(final PostSegmentSplitEvent event) {

        if(event.getSource() == this){

            belongsTo().getEventBus().unregister(this);
            _manager.freeUpBuffer(_mmap);
        }
    }

    @Subscribe
    @AllowConcurrentEvents
    protected void onPostInvalidateAll(final PostInvalidateAllEvent event){

        final Collection<Segment> abandons = event.getSource();

        if(abandons.contains(this)){

            _belongsTo.getEventBus().unregister(this);
            _manager.freeUpBuffer(_mmap);
        }
    }

    protected static final Predicate<Pair<Long, BinDocument>> _nonTombstonePredicate = new Predicate<Pair<Long, BinDocument>>() {

        public @Override boolean apply(final Pair<Long, BinDocument> input) {
            return !input.getValue1().isTombstone();
        }
    };

    protected LeafSegment newLeafSegment(final ByteBuffer buffer,
                                         final Range<Integer> range) throws IOException {

        return new LeafSegment(range, _belongsTo, _manager, buffer);
    }

}
