package org.ebaysf.bluewhale.storage;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.cache.RemovalCause;
import com.google.common.collect.*;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import org.brettw.SparseBitSet;
import org.ebaysf.bluewhale.configurable.Configuration;
import org.ebaysf.bluewhale.document.BinDocument;
import org.ebaysf.bluewhale.document.BinDocumentFactory;
import org.ebaysf.bluewhale.event.PersistenceRequiredEvent;
import org.ebaysf.bluewhale.event.PostExpansionEvent;
import org.ebaysf.bluewhale.event.PostInvestigationEvent;
import org.ebaysf.bluewhale.event.RequestInvestigationEvent;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by huzhou on 2/27/14.
 */
public class BinStorageImpl implements BinStorage {

    public static final long MASK_OFFSET = -1L >>> (Integer.SIZE + 1);
    public static final Comparator<BinJournal> ORDER_BY_LAST_MODIFIED = new Comparator<BinJournal>() {
        public @Override int compare(final BinJournal journalOne, final BinJournal journalTwo) {
            final long lastModifiedOfJournalOne = journalOne.usage().getLastModified();
            final long lastModifiedOfJournalTwo = journalTwo.usage().getLastModified();
            return lastModifiedOfJournalOne == lastModifiedOfJournalTwo ? 0 : (lastModifiedOfJournalOne > lastModifiedOfJournalTwo ? 1 : -1);
        }
    };

    private static final Logger LOG = LoggerFactory.getLogger(BinStorageImpl.class);

    private final transient Configuration _configuration;
    private final transient EventBus _eventBus;
    private final transient ExecutorService _executor;
    private final transient UsageTrack _usageTrack;
    private final transient ReentrantLock _lock;

    protected final transient JournalsManager _manager;
    protected final transient BinDocumentFactory _factory;

    protected volatile transient BinJournal _journaling;
    protected volatile RangeMap<Integer, BinJournal> _navigableJournals;
    protected volatile transient RangeSet<Integer> _dangerousJournals;
    protected volatile transient Future<?> _openInvestigation;

    public BinStorageImpl(final Configuration configuration,
                          final List<BinJournal> loadings,
                          final UsageTrack usageTrack) throws IOException {

        _configuration = Preconditions.checkNotNull(configuration);

        _factory = _configuration.getBinDocumentFactory();
        _eventBus = _configuration.getEventBus();
        _executor = _configuration.getExecutor();
        _usageTrack = Preconditions.checkNotNull(usageTrack);

        _navigableJournals = ImmutableRangeMap.of();
        _dangerousJournals = ImmutableRangeSet.of();

        _manager = new JournalsManager(_configuration);
        _eventBus.register(this);

        _lock = new ReentrantLock(true);

        warmUp(loadings);
    }

    public @Override File local() {

        return _configuration.getLocal();
    }

    public @Override long append(final BinDocument document) throws IOException {

        Preconditions.checkState(_journaling != null && _journaling.currentState().isWritable());

        final int offset = _journaling.append(document);

        if(offset == BinJournal.NEVER_GOING_TO_HAPPEN){
            throw new IllegalArgumentException("the document is too large to fit in a single blank journal!");
        }

        if(offset == BinJournal.INSUFFICIENT_JOURNAL_SPACE){

            final BinJournal previous = _journaling;
            try{
                _lock.lock();

                acceptWritable(nextWritable());
                return append(document);
            }
            finally{
                _eventBus.post(new PostExpansionEvent(this, previous));
                _lock.unlock();
            }
        }
        else{

            long token = _journaling.range().lowerEndpoint().longValue();
            token <<= Integer.SIZE;//move range to the highest 32 bits
            return token | offset;
        }
    }

    public @Override BinDocument read(final long token) throws IOException {

        if(token < 0L) return null;

        final BinJournal zone = route(token);
        final int offset = (int)(token & MASK_OFFSET);

        return Objects.firstNonNull(zone, EvictedBinJournal.INSTANCE).read(offset);
    }

    public @Override BinJournal route(final long token) {

        final int journalCode = (int)(token >> Integer.SIZE);

        return _navigableJournals.get(journalCode);
    }

    public @Override boolean isDangerous(final long token) {

        final int journalCode = (int)(token >> Integer.SIZE);

        return _dangerousJournals.contains(journalCode);
    }

    public @Override UsageTrack getUsageTrack() {

        return _usageTrack;
    }

    public @Override int getJournalLength() {

        return _configuration.getJournalLength();
    }

    public @Override int getMaxJournals() {

        return _configuration.getMaxJournals();
    }

    public @Override int getMaxMemoryMappedJournals(){
        return _configuration.getMaxMemoryMappedJournals();
    }

    public @Override Iterator<BinJournal> iterator() {

        final Set<BinJournal> orderedByLastModified = Sets.newTreeSet(ORDER_BY_LAST_MODIFIED);
        for(BinJournal journal : _navigableJournals.asMapOfRanges().values()){
            orderedByLastModified.add(journal);
        }

        LOG.debug("[storage] iteration ordered: {}", orderedByLastModified);
        return orderedByLastModified.iterator();
    }

    protected void warmUp(final List<BinJournal> loadings) throws IOException {

        final ImmutableRangeMap.Builder<Integer, BinJournal> builder = ImmutableRangeMap.builder();

        for(BinJournal cold : loadings) {

            if(!cold.currentState().isEvicted()){

                try {
                    final BinJournal warm = cold.currentState().isMemoryMapped()
                            ? new ByteBufferBinJournal(cold.local(), cold.currentState(),
                                cold.range(), _manager, cold.usage(), _factory, cold.getJournalLength(), cold.getDocumentSize(),
                                _manager.loadBuffer(cold.local()).getValue1())
                            : new FileChannelBinJournal(cold.local(),
                                cold.range(), _manager, cold.usage(), _factory, cold.getJournalLength(), cold.getDocumentSize(),
                                -1);

                    builder.put(warm.range(), warm);
                }
                catch (IOException e) {
                    LOG.error("cold cache warm up failed", e);
                }
            }
        }

        _navigableJournals = builder.build();

        acceptWritable(nextWritable());

        _eventBus.post(new PersistenceRequiredEvent(this));
    }

    protected WriterBinJournal nextWritable() throws IOException {

        final int journalCode = _navigableJournals.asMapOfRanges().isEmpty() ? 0 : (_navigableJournals.span().upperEndpoint().intValue() + 1) % Integer.MAX_VALUE;

        final Range<Integer> range = Range.singleton(journalCode);

        final Pair<File, ByteBuffer> next = _manager.newBuffer();

        return new WriterBinJournal(next.getValue0(), range, _manager, new JournalUsageImpl(System.nanoTime(), 0), _factory, _configuration.getJournalLength(), next.getValue1());
    }

    protected void acceptWritable(final WriterBinJournal writable) {

        final ImmutableRangeMap.Builder<Integer, BinJournal> builder = ImmutableRangeMap.builder();

        for(Map.Entry<Range<Integer>, BinJournal> entry : _navigableJournals.asMapOfRanges().entrySet()) {

            builder.put(entry.getKey(), entry.getValue());
        }

        builder.put(writable.range(), writable);

        _navigableJournals = builder.build();

        _journaling = writable;

        LOG.info("[storage] new writable created, navigableJournals updated: {}", _navigableJournals);
    }

    protected BinJournal downgrade(final BinJournal journal) throws FileNotFoundException {

        Preconditions.checkArgument(journal != null && journal.currentState().isMemoryMapped());

        //this doesn't happen immediately! some journals might still be using the buffer
        //it only takes effect after the journal using the buffer gets garbage collected
        _manager.freeUpBuffer(journal.getMemoryMappedBuffer());

        LOG.info("[storage] downgrade journal:{} to FileChannelBinJournal", journal);

        return new FileChannelBinJournal(journal.local(),
                journal.range(),
                _manager,
                journal.usage(),
                _factory,
                journal.getJournalLength(),
                journal.getDocumentSize(),
                -1);
    }

    protected BinJournal immutable(final BinJournal previous) {

        Preconditions.checkArgument(previous != null && previous.currentState().isWritable());

        return new ByteBufferBinJournal(previous.local(),
                BinJournal.JournalState.BufferedReadOnly,
                previous.range(),
                _manager,
                new JournalUsageImpl(previous.usage().getLastModified(), previous.getDocumentSize()),
                _factory,
                previous.getJournalLength(),
                previous.getDocumentSize(),
                previous.getMemoryMappedBuffer());
    }

    @Subscribe
    public void postExpansion(final PostExpansionEvent event) {

        LOG.debug("[storage] post expansion processing");

        final BinJournal previous = event.getPreviousWritable();
        //lots of work here:
        //downgrade previous writable
        //inspect journals to update usages
        //determine if compression must be triggered
        //determine if eviction must be triggered
        Preconditions.checkArgument(previous.currentState().isWritable());

        final ImmutableRangeMap.Builder<Integer, BinJournal> navigableBuilder = ImmutableRangeMap.builder();
        final ImmutableRangeSet.Builder<Integer> dangerBuilder = ImmutableRangeSet.builder();

        final Map<Range<Integer>, BinJournal> downgradeCandidates = Maps.newLinkedHashMap();
        //iterate by last modified
        int age = 0, numOfJournals = _navigableJournals.asMapOfRanges().size();

        //things only get dangerous when the journals are full.
        if(numOfJournals == _configuration.getMaxJournals()){
            for(BinJournal journal : this) {
                //at least the oldest journal is considered dangerous.
                if(age == 0){
                    dangerBuilder.add(journal.range());
                }
                else if((age += 1) < _configuration.getDangerousJournalsRatio() * numOfJournals){
                    dangerBuilder.add(journal.range());
                }
                else{
                    break;
                }
            }
        }

        //now we need to examine possible downgrades.
        for(BinJournal journal : this) {
            //only ByteBufferReadOnly journals are candidates for downgrades.
            if(journal.currentState().isMemoryMapped()){
                if(!journal.currentState().isWritable()){
                    downgradeCandidates.put(journal.range(), journal);
                }
            }
            //others need go back to _navigableJournals as is. FileChannelReadOnly
            else{
                navigableBuilder.put(journal.range(), journal);
            }
        }

        //check for downgrades
        int downgrades = downgradeCandidates.size() - getMaxMemoryMappedJournals() + 1/*one for the writable*/;
        LOG.info("[storage] downgrades:{} {}", downgrades, downgradeCandidates.values());
        for(Iterator<Map.Entry<Range<Integer>, BinJournal>> it = downgradeCandidates.entrySet().iterator(); it.hasNext(); downgrades -= 1){

            final Map.Entry<Range<Integer>, BinJournal> entry = it.next();
            try {
                navigableBuilder.put(entry.getKey(), downgrades > 0 ? downgrade(entry.getValue()) : entry.getValue());
            }
            catch (FileNotFoundException e) {
                LOG.error("downgrade failed", e);
            }
        }

        //make previous writable readonly
        LOG.info("[storage] make previous writable immutable");
        navigableBuilder.put(previous.range(), immutable(previous));

        //keep writable in it
        navigableBuilder.put(_journaling.range(), _journaling);

        _navigableJournals = navigableBuilder.build();
        _dangerousJournals = dangerBuilder.build(); //rebuild dangerous journals set

        _eventBus.post(new PersistenceRequiredEvent(this));
        _eventBus.post(new RequestInvestigationEvent(this));
    }

    @Subscribe
    public void onRequestedInvestigation(final RequestInvestigationEvent event) {

        if(_openInvestigation != null || event.getSource() != this){
            return;
        }

        //there're cannot be 2 open investigations at the same time, too much cost
        _openInvestigation = _executor.submit(new Runnable() {

            public @Override void run() {

                final ListMultimap<InspectionReport, BinJournal> investigation = Multimaps.newListMultimap(
                        Maps.<InspectionReport, Collection<BinJournal>>newHashMap(),
                        new Supplier<List<BinJournal>>() {
                            public @Override List<BinJournal> get() {
                                return Lists.newLinkedList();
                            }
                        });

                int forced = _navigableJournals.asMapOfRanges().size() - getMaxJournals();

                for(BinJournal journal : BinStorageImpl.this){

                    if(!journal.currentState().isMemoryMapped()){

                        if((forced -= 1) >= 0){
                            LOG.info("[storage] forced out aging journal: {}", journal);
                            investigation.put(InspectionReport.EvictionRequiredBySize, journal);
                            continue;
                        }

                        if(expired(journal)){
                            LOG.info("[storage] expired aging journal: {}", journal);
                            investigation.put(InspectionReport.EvictionRequiredByTTL, journal);
                            continue;
                        }

                        LOG.info("[storage] make investigation on aging journal: {}", journal);

                        final JournalUsage usage = journal.usage();
                        final SparseBitSet alives = usage.getAlives();
                        int index = 0;

                        for(Iterator<BinDocument> it = journal.iterator(); it.hasNext(); index += 1){
                            final BinDocument suspect = it.next();
                            if(alives.get(index)){
                                alives.set(index, _usageTrack.using(suspect));
                            }
                            if(usage.isUsageRatioAbove(_configuration.getLeastJournalUsageRatio())){
                                break;//no more investigation needed upon this journal till next time.
                            }
                        }

                        if(usage.isAllDead()){
                            investigation.put(InspectionReport.EvictionRequiredBySize, journal);
                        }
                        else if(!usage.isUsageRatioAbove(_configuration.getLeastJournalUsageRatio())){
                            investigation.put(InspectionReport.CompressionRequired, journal);
                        }
                        else{
                            investigation.put(InspectionReport.RemainAsIs, journal);
                        }
                    }
                }

                LOG.info("[storage] investigation finished, report: {}", investigation);

                _eventBus.post(new PostInvestigationEvent(investigation));
            }

            private boolean expired(final BinJournal journal) {
                final Pair<Long, TimeUnit> ttl = _configuration.getTTL();
                return ttl != null
                        && System.nanoTime() - journal.usage().getLastModified() > ttl.getValue1().toNanos(ttl.getValue0().longValue());
            }
        });
    }

    @Subscribe
    public void onPostInvestigation(final PostInvestigationEvent event){
        try {
            final ListMultimap<InspectionReport, BinJournal> report = event.getSource();

            LOG.info("[storage] processing investigation report: {}", report);
            final Set<Range<Integer>> exclusions = Sets.newIdentityHashSet();
            for(BinJournal journal : Objects.firstNonNull(report.get(InspectionReport.EvictionRequiredBySize), Collections.<BinJournal>emptyList())){
                for(BinDocument evict: journal){
                    if(_usageTrack.using(evict)){
                        _usageTrack.forget(evict, RemovalCause.SIZE);
                    }
                }
                exclusions.add(journal.range());
            }

            for(BinJournal journal : Objects.firstNonNull(report.get(InspectionReport.EvictionRequiredByTTL), Collections.<BinJournal>emptyList())){
                for(BinDocument evict: journal){
                    if(_usageTrack.using(evict)){
                        _usageTrack.forget(evict, RemovalCause.EXPIRED);
                    }
                }
                exclusions.add(journal.range());
            }

            for(BinJournal journal : Objects.firstNonNull(report.get(InspectionReport.CompressionRequired), Collections.<BinJournal>emptyList())){
                for(BinDocument compress: journal){
                    if(_usageTrack.using(compress)){
                        _usageTrack.refresh(compress);
                    }
                }
                exclusions.add(journal.range());
            }

            _openInvestigation = null;

            _lock.lock();

            final ImmutableRangeMap.Builder<Integer, BinJournal> navigableBuilder = ImmutableRangeMap.builder();
            final ImmutableRangeSet.Builder<Integer> dangerousBuilder = ImmutableRangeSet.builder();

            for(Map.Entry<Range<Integer>, BinJournal> entry : _navigableJournals.asMapOfRanges().entrySet()) {
                if(!exclusions.contains(entry.getKey())){
                    navigableBuilder.put(entry.getKey(), entry.getValue());
                }
            }

            for(Range<Integer> entry : _dangerousJournals.asRanges()) {
                if(!exclusions.contains(entry)){
                    dangerousBuilder.add(entry);
                }
            }

            _navigableJournals = navigableBuilder.build();
            _dangerousJournals = dangerousBuilder.build();
            _eventBus.post(new PersistenceRequiredEvent(this));

        }
        catch(Exception ex){
            LOG.error("investigation processing failed", ex);
        }
        finally {
            _lock.unlock();
        }
    }

}
