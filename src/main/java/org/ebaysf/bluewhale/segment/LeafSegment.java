package org.ebaysf.bluewhale.segment;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.cache.AbstractCache;
import com.google.common.cache.RemovalCause;
import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.Subscribe;
import org.ebaysf.bluewhale.command.Get;
import org.ebaysf.bluewhale.command.Put;
import org.ebaysf.bluewhale.command.PutAsIs;
import org.ebaysf.bluewhale.command.PutAsRefresh;
import org.ebaysf.bluewhale.configurable.Configuration;
import org.ebaysf.bluewhale.document.BinDocument;
import org.ebaysf.bluewhale.event.*;
import org.ebaysf.bluewhale.serialization.Serializer;
import org.ebaysf.bluewhale.storage.BinStorage;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by huzhou on 2/28/14.
 */
public class LeafSegment extends AbstractSegment {

    private static final Logger LOG = LoggerFactory.getLogger(LeafSegment.class);

    private final transient ByteBuffer _mmap;
    private final transient LongBuffer _tokens;

    public LeafSegment(final File local,
                       final Range<Integer> range,
                       final Configuration configuration,
                       final SegmentsManager manager,
                       final BinStorage storage,
                       final ByteBuffer mmap,
                       final int size) {

        super(local, range, configuration, manager, storage, size);

        _mmap = mmap;
        _tokens = _mmap.asLongBuffer();

        configuration().getEventBus().register(this);
        _manager.rememberBufferUsedBySegment(Pair.with(local, _mmap), this);
    }

    public @Override <V> V get(final Get get) throws ExecutionException, IOException {
        //promised! no locking on Get
        if(isLeaf()){

            final int offset = getOffset(get.getHashCode());
            final long head = _tokens.get(offset);
            final AbstractCache.StatsCounter statsCounter = get.getStatsCounter();

            final V hit = getIfPresent(get, offset, head);
            if(hit != null){
                statsCounter.recordHits(1);
                return hit;
            }
            if(!get.loadIfAbsent() || get.getValueLoader() == null){
                return null;
            }

            statsCounter.recordMisses(1);
            final long beforeLoad = System.nanoTime();
            try{

                _lock.lock();

                if(_tokens.get(offset) != head){
                    final V loadedByOthers = getIfPresent(get, offset, head);
                    if(loadedByOthers != null){
                        return loadedByOthers;
                    }
                }

                final Future<V> value = configuration().getExecutor().submit(get.<V>getValueLoader());
                final V resolved = value.get();
                put(new PutAsIs(get.getKey(), resolved, get.getHashCode()));

                statsCounter.recordLoadSuccess(System.nanoTime() - beforeLoad);
                //we do the blocking put, and then try calling get again
                return resolved;

            }
            catch(InterruptedException e){
                statsCounter.recordLoadException(System.nanoTime() - beforeLoad);
                throw new ExecutionException(e);//value get failure
            }
            finally {
                _lock.unlock();
            }
        }

        return super.get(get);
    }

    public @Override void put(final Put put) throws IOException {
        try{
            _lock.lock();
            if(isLeaf()){

                final int offset = getOffset(put.getHashCode());
                final long next = _tokens.get(offset);
                if(isPutObsolete(put, next)){
                    return;//this is because of the background optimization tasks
                }

                final BinStorage storage = getStorage();
                final BinDocument document = put.create(getKeySerializer(), getValSerializer(), next);
                final long token = storage.append(document);
                _tokens.put(offset, token);

                //we must check the size change here, then trigger possible splits
                evaluateEffectOfPut(put, next);

                _configuration.getEvictionStrategy().afterPut(this, storage, token, document);
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
            final long head = _tokens.get(getOffset(suspect.getHashCode()));
            try {
                return head >= 0 && using(suspect.getKey(), head, suspect.getLastModified(), suspect.getNext());
            }
            catch (IOException e) {
                LOG.error("usage tracking failed", e);
                return true;
            }
        }
        return super.using(suspect);
    }

    public @Override void forget(final BinDocument obsolete,
                                 final RemovalCause cause) {

        if(isLeaf()){
            //the document is being evicted, tell user about the removal
            configuration().getEventBus().post(new RemovalNotificationEvent(obsolete, cause));
        }
        else{
            super.forget(obsolete, cause);
        }
    }

    public @Override void refresh(final BinDocument origin) {
        if(isLeaf()){
            //the document is likely to be evicted, but still useful, write the same to the latest journal
            //no path shortening till it's read
            try {
                put(new PutAsRefresh(origin.getKey(), origin.getValue(), origin.getHashCode(), origin.getState()));
            }
            catch (IOException e) {
                LOG.error("refresh document failed", e);
            }
        }
        else{
            super.refresh(origin);
        }
    }

    protected boolean using(final ByteBuffer keyAsBytes,
                            final long head,
                            final long lastModified,
                            final long next) throws IOException {

        final BinStorage storage = getStorage();
        final Serializer keySerializer = getKeySerializer();

        //check each doc in the path, including the suspect itself, if there's any element of equivalent key
        //compare their last modified time, if there's newer document than suspect, return using=false, otherwise true
        for(BinDocument doc = storage.read(head); doc != null; doc = storage.read(doc.getNext())){
            if(keySerializer.equals(keyAsBytes.duplicate(), doc.getKey())){
                return lastModified >= doc.getLastModified() && next == doc.getNext();
            }
        }
        //shouldn't ever happen, because the document should be compared with itself in the loop above
        return false;
    }

    protected <V> V getIfPresent(final Get get,
                                 final int offset,
                                 final long head) throws IOException{

        final BinStorage storage = getStorage();
        final Serializer keySerializer = getKeySerializer();
        final Object key = get.getKey();

        int length = 0;
        long token = head;
        try{
            for(BinDocument doc = storage.read(token); doc != null; token = doc.getNext(), doc = storage.read(token), length += 1){
                if(keySerializer.equals(key, doc.getKey())){
                    _configuration.getEvictionStrategy().afterGet(this, storage, token, doc);
                    return doc.isTombstone() ? null : (V)getValSerializer().deserialize(doc.getValue(), doc.isCompressed());
                }
            }
            return null;
        }
        finally {
            if(length >= _configuration.getMaxPathDepth()){
                configuration().getEventBus().post(new PathTooLongEvent(this, offset, head));
            }
        }
    }

    /**
     * check if a put should be rejected
     * @param next
     * @param put
     * @return
     * @throws IOException
     */
    protected boolean isPutObsolete(final Put put,
                                    final long next) throws IOException {

        if(!put.refreshes()){
            return false;
        }

        final BinStorage storage = getStorage();
        //verify normal updates is ok
        for(BinDocument doc = storage.read(next); doc != null; doc = storage.read(doc.getNext())) {

            if(getKeySerializer().equals(put.getKey(getKeySerializer()), doc.getKey())) {
                return put.getLastModified() < doc.getLastModified();//the PUT is created ahead of the existing docs, filter it out
            }
        }
        return false;
    }

    /**
     * evaluate after Put, the change of _size, and whether RemovalNotificationEvent must be raised. then whether a #split will be needed.
     * @param put
     * @param next
     * @throws IOException
     */
    protected void evaluateEffectOfPut(final Put put,
                                       final long next) throws IOException {

        if(put.refreshes()){
            return;//refresh should have no effect on the data, size shouldn't change, nothing got removed either.
        }

        if(next < 0L){
            _size += put.invalidates() ? 0 : 1;
            //next token points to no one. size increases if PutAsIs, no change if PutAsInvalidate (users' error tolerated)
            return;
        }

        final boolean noMoreSplit = range().upperEndpoint() - range().lowerEndpoint() <= _manager.getSpanAtLeast();

        final BinStorage storage = getStorage();
        final Object key = put.getKey(getKeySerializer());

        for(BinDocument doc = storage.read(next); doc != null; doc = storage.read(doc.getNext())){

            if(getKeySerializer().equals(key, doc.getKey())){

                if(!put.invalidates() && !doc.isTombstone()){
                    //put overwrites some old value
                    configuration().getEventBus().post(new RemovalNotificationEvent(doc, RemovalCause.REPLACED));
                }
                else if(!put.invalidates() && doc.isTombstone()){
                    //put is conceptually new
                    _size += 1;

                    if(!noMoreSplit && _size > MAX_TOKENS_IN_ONE_SEGMENT){
                        split();
                    }
                }
                else if(put.invalidates() && !doc.isTombstone()){
                    //put invalidates some old value
                    configuration().getEventBus().post(new RemovalNotificationEvent(doc, RemovalCause.EXPLICIT));
                }
                else{
                    //put.invalidate() && doc.isTombstone()
                    //nothing should happen!
                }
                return;
            }
        }

        //nothing overwritten, this is the 1st time this key was created.
        if(!put.invalidates()){

            _size += 1;

            if(!noMoreSplit && _size > MAX_TOKENS_IN_ONE_SEGMENT){
                split();
            }
        }
    }

    protected void split() throws IOException {

        LOG.debug("[segment] split into lower & upper");

        final int lowerBound = range().lowerEndpoint();
        final int upperBound = range().upperEndpoint();

        Preconditions.checkArgument(lowerBound != upperBound, "cannot further split!");

        final int splitAt = lowerBound + ((upperBound - lowerBound) >> 1);//(int)(((long)lowerBound +(long)upperBound) >> 1L);
        final BinStorage storage = getStorage();

        final LeafSegment lower = newLeafSegment(_manager.allocateBuffer(), Range.closed(lowerBound, splitAt));
        final LeafSegment upper = newLeafSegment(_manager.allocateBuffer(), Range.closed(splitAt + 1, upperBound));

        for(int offset = 0, len = _tokens.capacity(); offset < len; offset += 1){
            final long token = _tokens.get(offset);
            if(token >= 0){
                final Map<Object, Pair<Long, BinDocument>> groupByKey = Maps.newLinkedHashMap();
                //1st, filter all docs, keeping only the 1st doc of each unique key
                long nextToken = token;
                for(BinDocument doc = storage.read(nextToken); doc != null; nextToken = doc.getNext(), doc = storage.read(nextToken)){

                    final Object key = getKeySerializer().deserialize(doc.getKey(), false);
                    if(!groupByKey.containsKey(key)){
                        groupByKey.put(key, Pair.with(Long.valueOf(nextToken), doc));
                    }
                }

                //2nd, sweep the filtered documents map again, remove all tombstones, those keys were simply invalidated
                //3rd, actual split, including size calculations.
                boolean lowerHeadGiven = false, upperHeadGiven = false;
                for(Pair<Long, BinDocument> survival : Collections2.filter(groupByKey.values(), _nonTombstonePredicate)){
                    final int segmentCode = getSegmentCode(survival.getValue1().getHashCode());
                    if(lower.range().contains(segmentCode)){
                        if(!lowerHeadGiven){
                            lower._tokens.put(offset, survival.getValue0().longValue());
                            lowerHeadGiven = true;
                        }
                        lower._size += 1;
                    }
                    else if(upper.range().contains(segmentCode)){
                        if(!upperHeadGiven){
                            upper._tokens.put(offset, survival.getValue0().longValue());
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
        configuration().getEventBus().post(new SegmentSplitEvent(this, getChildren()));
    }

    @Subscribe
    public void onPostSegmentSplit(final PostSegmentSplitEvent event) {

        if(event.getSource() == this){

            LOG.info("[segment] {} => {} & {}", this, _lower, _upper);
            _manager.freeUpBuffer(Pair.with(local(), _mmap));
        }
    }

    @Subscribe
    @AllowConcurrentEvents
    public void onPostInvalidateAll(final PostInvalidateAllEvent event){

        final Collection<Segment> abandons = event.getSource();

        if(abandons.contains(this)){

            LOG.info("[segment] invalidate all happens");
            configuration().getEventBus().unregister(this);
            _manager.freeUpBuffer(Pair.with(local(), _mmap));
        }
    }

    @Subscribe
    public void onPathTooLong(final PathTooLongEvent event){

        if(event.getSource() != this){
            return;
        }

        LOG.debug("[segment] path too long, optimization triggered");

        try{
            _lock.lock();

            final int offset = event.getOffset();
            final long headTokenExpected = event.getHeadToken();

            long token = _tokens.get(offset);
            if(token != headTokenExpected){
                return;//already changed
            }

            final BinStorage storage = getStorage();
            final Set<ByteBuffer> knowns = Sets.newTreeSet(new KeyComparator(getKeySerializer()));
            final Stack<BinDocument> survivals = new Stack<BinDocument>();

            int pathDepth = 0;
            //go through the path, push 1st met (& none tombstone) as survivals
            //this will exclude all tombstones, and all obsolete values
            for(BinDocument doc = storage.read(token); doc != null; doc = storage.read(doc.getNext())){
                if(!knowns.contains(doc.getKey())){
                    knowns.add(doc.getKey());
                    if(!doc.isTombstone()){//already removed, no need to rebuild
                        survivals.add(doc);
                    }
                }
                pathDepth += 1;
            }

            //when survivals are less than half of the path depth, this includes a corner case when there's no survivals at all
            if(survivals.size() < pathDepth / 2){
                long next = -1L;

                //note, this is a stack pop, therefore LIFO, not really a must (key dedup happened), but fits better the actual write order
                while(!survivals.empty()){
                    final BinDocument survival = survivals.pop();
                    next = storage.append(new PutAsRefresh(survival.getKey(), survival.getValue(), survival.getHashCode(), survival.getState())
                            .create(getKeySerializer(), getValSerializer(), next));
                }
                //validate if the headToken is untouched.
                if(headTokenExpected == _tokens.get(offset)){
                    _tokens.put(offset, next);
                }
            }
        }
        catch(IOException e){
            LOG.error("path shortening failed", e);
        }
        finally {
            _lock.unlock();
        }
    }

    protected LeafSegment newLeafSegment(final Pair<File, ByteBuffer> allocate,
                                         final Range<Integer> range) throws IOException {

        return new LeafSegment(allocate.getValue0(), range, configuration(), _manager, _storage, allocate.getValue1(), 0);
    }

    protected static final Predicate<Pair<Long, BinDocument>> _nonTombstonePredicate = new Predicate<Pair<Long, BinDocument>>() {

        public @Override boolean apply(final Pair<Long, BinDocument> input) {
            return !input.getValue1().isTombstone();
        }
    };

    protected static final class KeyComparator implements Comparator<ByteBuffer> {

        private final Serializer<?> _keySerializer;

        public KeyComparator(final Serializer<?> keySerializer){

            _keySerializer = keySerializer;
        }

        public @Override int compare(final ByteBuffer o1, final ByteBuffer o2) {
            if(_keySerializer.equals(o1, o2)){
                return 0;
            }
            else{
                return o1.hashCode() - o2.hashCode();
            }
        }
    }

}
