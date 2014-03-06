package org.ebaysf.bluewhale.segment;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.cache.RemovalCause;
import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.Subscribe;
import org.ebaysf.bluewhale.Cache;
import org.ebaysf.bluewhale.command.Get;
import org.ebaysf.bluewhale.command.Put;
import org.ebaysf.bluewhale.command.PutAsIs;
import org.ebaysf.bluewhale.command.PutAsRefresh;
import org.ebaysf.bluewhale.document.BinDocument;
import org.ebaysf.bluewhale.event.*;
import org.ebaysf.bluewhale.serialization.Serializer;
import org.ebaysf.bluewhale.storage.BinStorage;
import org.javatuples.Pair;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Logger;

/**
 * Created by huzhou on 2/28/14.
 */
public class LeafSegment extends AbstractSegment {

    private static final Logger LOG = Logger.getLogger(LeafSegment.class.getName());

    private final ByteBuffer _mmap;
    private final LongBuffer _tokens;

    public LeafSegment(final Range<Integer> range,
                       final Cache<?, ?> belongsTo,
                       final SegmentsManager manager,
                       final ByteBuffer mmap) {

        super(range, belongsTo, manager);

        _mmap = mmap;
        _tokens = _mmap.asLongBuffer();

        _belongsTo.getEventBus().register(this);
        _manager.rememberBufferUsedBySegment(_mmap, this);
    }

    public @Override <V> V get(Get get) throws ExecutionException, IOException {
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
                else if(!put.refreshes() && next >= 0){//we won't do path shortening till read spotted the long paths
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
                LOG.warning(Throwables.getStackTraceAsString(e));
                return true;
            }
        }
        return super.using(suspect);
    }

    public @Override void forget(final BinDocument obsolete, final RemovalCause cause) {

        if(isLeaf()){

            _belongsTo.getEventBus().post(new RemovalNotificationEvent(obsolete, cause));
        }
        else{
            super.forget(obsolete, cause);
        }
    }

    public @Override void refresh(final BinDocument origin) {

        if(isLeaf()){
            try {
                put(new PutAsRefresh(origin.getKey(), origin.getValue(), origin.getHashCode(), origin.getState()));
            }
            catch (IOException e) {
                LOG.warning(Throwables.getStackTraceAsString(e));
            }
        }
        else{
            super.refresh(origin);
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
            if(length >= SHORTEN_PATH_THRESHOLD){
                _belongsTo.getEventBus().post(new PathTooLongEvent(this, offset, _tokens.get(offset)));
            }
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
    protected boolean isPutObsolete(final long next, final Put put) throws IOException {

        if(!put.refreshes()){
            return false;
        }

        final BinStorage bin = getStorage();
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
        //refreshes/invalidates doesn't increase the size!
        if(put.refreshes()
                || put.invalidates()
                || range().upperEndpoint() - range().lowerEndpoint() <= _manager.getSpanAtLeast()) {

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

        LOG.info("[segment] split into lower & upper");

        final int lowerBound = range().lowerEndpoint();
        final int upperBound = range().upperEndpoint();

        Preconditions.checkArgument(lowerBound != upperBound, "cannot further split!");

        final int splitAt = lowerBound + ((upperBound - lowerBound) >> 1);//(int)(((long)lowerBound +(long)upperBound) >> 1L);
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
                LOG.warning(Throwables.getStackTraceAsString(e));
            }
        }
    }

    @Subscribe
    public void onPostSegmentSplit(final PostSegmentSplitEvent event) {

        if(event.getSource() == this){

            LOG.info(String.format("[segment] split occurred %s -> %s,%s", this, _lower, _upper));
            _manager.freeUpBuffer(_mmap);
        }
    }

    @Subscribe
    @AllowConcurrentEvents
    public void onPostInvalidateAll(final PostInvalidateAllEvent event){

        final Collection<Segment> abandons = event.getSource();

        if(abandons.contains(this)){

            LOG.info("[segment] invalidate all");
            _belongsTo.getEventBus().unregister(this);
            _manager.freeUpBuffer(_mmap);
        }
    }

    @Subscribe
    public void onPathTooLong(final PathTooLongEvent event){

        if(event.getSource() == this){

            LOG.info("[segment] path too long, optimization triggered");

            final int offset = event.getOffset();
            final long headTokenExpected = event.getHeadToken();

            long token = _tokens.get(offset);
            if(token != headTokenExpected){
                return;//already changed
            }

            final BinStorage storage = getStorage();
            final Serializer<Object> keySerializer = getKeySerializer();
            final Set<ByteBuffer> exists = Sets.newTreeSet(new Comparator<ByteBuffer>() {
                public @Override int compare(ByteBuffer o1, ByteBuffer o2) {
                    if(keySerializer.equals(o1, o2)){
                        return 0;
                    }
                    else{
                        return o1.hashCode() - o2.hashCode();
                    }
                }
            });

            final Stack<BinDocument> lifo = new Stack<BinDocument>();
            int walkThrough = 0;
            try{
                for(BinDocument doc = storage.read(token); doc != null; doc = storage.read(doc.getNext())){
                    if(!exists.contains(doc.getKey())){
                        exists.add(doc.getKey());
                        if(!doc.isTombstone()){//already removed, no need to rebuild
                            lifo.add(doc);
                        }
                    }
                    walkThrough += 1;
                }

                if(!lifo.empty() && lifo.size() < walkThrough / 2){
                    //a shorten path is to happen
                    long next = -1L;

                    while(!lifo.isEmpty()){
                        final BinDocument reset = lifo.pop();
                        next = storage.append(new PutAsRefresh(reset.getKey(), reset.getValue(), reset.getHashCode(), reset.getState()).create(getKeySerializer(), getValSerializer(), next));
                    }

                    try{
                        _lock.lock();
                        if(headTokenExpected == _tokens.get(offset)){
                            _tokens.put(offset, next);
                        }
                    }
                    finally {
                        _lock.unlock();
                    }
                }

            }
            catch(IOException e){
                LOG.warning(Throwables.getStackTraceAsString(e));
            }
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
