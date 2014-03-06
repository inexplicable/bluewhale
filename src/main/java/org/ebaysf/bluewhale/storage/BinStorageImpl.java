package org.ebaysf.bluewhale.storage;

import com.google.common.base.Objects;
import com.google.common.base.*;
import com.google.common.cache.RemovalCause;
import com.google.common.collect.*;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import org.brettw.SparseBitSet;
import org.ebaysf.bluewhale.document.BinDocument;
import org.ebaysf.bluewhale.document.BinDocumentFactory;
import org.ebaysf.bluewhale.event.PostExpansionEvent;
import org.ebaysf.bluewhale.event.PostInvestigationEvent;
import org.javatuples.Pair;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

/**
 * Created by huzhou on 2/27/14.
 */
public class BinStorageImpl implements BinStorage {

    public static final long MASK_OFFSET = -1L >>> (Integer.SIZE + 1);
    public static final Predicate<BinJournal> JOURNAL_EVICTED = new Predicate<BinJournal>() {
        @Override
        public boolean apply(BinJournal input) {
            return input.currentState().isEvicted();
        }
    };

    private static final Logger LOG = Logger.getLogger(BinStorageImpl.class.getName());

    private final File _local;
    private final int _journalLength;
    private final int _maxJournals;
    private final int _maxMemoryMappedJournals;
    private final boolean _cleanUpOnExit;
    private final EventBus _eventBus;
    private final ExecutorService _executor;
    private final UsageTrack _usageTrack;
    private final ReentrantLock _lock;

    protected final JournalsManager _manager;
    protected final BinDocumentFactory _factory;

    protected volatile BinJournal _journaling;
    protected volatile RangeMap<Integer, BinJournal> _navigableJournals;
    protected volatile Future<?> _openInvestigation;

    public BinStorageImpl(final File local,
                          final BinDocumentFactory factory,
                          final int journalLength,
                          final int maxJournals,
                          final int maxMemoryMappedJournals,
                          final boolean cleanUpOnExit,
                          final List<BinJournal> loadings,
                          final EventBus eventBus,
                          final ExecutorService executor,
                          final UsageTrack usageTrack) throws IOException {

        _local = Objects.firstNonNull(local, com.google.common.io.Files.createTempDir());
        _factory = Preconditions.checkNotNull(factory);
        _eventBus = Preconditions.checkNotNull(eventBus);
        _executor = Preconditions.checkNotNull(executor);
        _usageTrack = Preconditions.checkNotNull(usageTrack);

        Preconditions.checkArgument(journalLength > 0);
        Preconditions.checkArgument(maxJournals > 1);
        Preconditions.checkArgument(maxMemoryMappedJournals > 1 && maxMemoryMappedJournals < maxJournals);

        _journalLength = journalLength;
        _maxJournals = maxJournals;
        _maxMemoryMappedJournals = maxMemoryMappedJournals;
        _cleanUpOnExit = cleanUpOnExit;
        _navigableJournals = ImmutableRangeMap.of();

        _manager = new JournalsManager(_local, _journalLength, _cleanUpOnExit, _eventBus, _executor);
        _eventBus.register(this);

        _lock = new ReentrantLock(true);

        warmUp(loadings);
        acceptWritable(nextWritable(_local));
    }

    public @Override File local() {

        return _local;
    }

    public @Override long append(BinDocument binDocument) throws IOException {

        Preconditions.checkState(_journaling != null && _journaling.currentState().isWritable());

        final int offset = _journaling.append(binDocument);

        if(offset < 0){

            final BinJournal previous = _journaling;
            try{
                _lock.lock();

                acceptWritable(nextWritable(_local));
                return append(binDocument);
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

        if(token < 0) return null;

        final BinJournal zone = route(token);
        final int offset = (int)(token & MASK_OFFSET);

        return Objects.firstNonNull(zone, EvictedBinJournal.INSTANCE).read(offset);
    }

    public @Override BinJournal route(final long token) {

        final int journalCode = (int)(token >> Integer.SIZE);

        return _navigableJournals.get(journalCode);
    }

    public @Override UsageTrack getUsageTrack() {

        return _usageTrack;
    }

    public @Override int getJournalLength() {

        return _journalLength;
    }

    public @Override int getMaxJournals() {

        return _maxJournals;
    }

    public @Override int getMaxMemoryMappedJournals(){
        return _maxMemoryMappedJournals;
    }

    public @Override int getEvictedJournals() {

        return Collections2.filter(_navigableJournals.asMapOfRanges().values(), JOURNAL_EVICTED).size();
    }

    public @Override Iterator<BinJournal> iterator() {

        final List<BinJournal> orderedByLastModified = Lists.newArrayList(_navigableJournals.asMapOfRanges().values());

        Collections.sort(orderedByLastModified, new Comparator<BinJournal>() {
            public @Override int compare(final BinJournal journalOne, final BinJournal journalTwo) {
                final long lastModifiedOfJournalOne = journalOne.usage().getLastModified();
                final long lastModifiedOfJournalTwo = journalTwo.usage().getLastModified();
                return lastModifiedOfJournalOne == lastModifiedOfJournalTwo ? 0 : (lastModifiedOfJournalOne > lastModifiedOfJournalTwo ? 1 : -1);
            }
        });

        LOG.info(String.format("[storage] iteration ordered:%s", orderedByLastModified));
        return orderedByLastModified.iterator();
    }

    protected RangeMap<Integer, BinJournal> warmUp(final List<BinJournal> loadings) {

        final ImmutableRangeMap.Builder<Integer, BinJournal> builder = ImmutableRangeMap.builder();

        for(BinJournal cold : loadings) {

            if(!cold.currentState().isEvicted()){

                builder.put(cold.range(), cold);
            }
        }

        return builder.build();
    }

    protected WriterBinJournal nextWritable(final File dir) throws IOException {

        final int journalCode = _navigableJournals.asMapOfRanges().isEmpty() ? 0 : (_navigableJournals.span().upperEndpoint().intValue() + 1) % Integer.MAX_VALUE;

        final Range<Integer> range = Range.singleton(journalCode);

        final Pair<File, ByteBuffer> next = _manager.newBuffer();

        return new WriterBinJournal(next.getValue0(), range, _manager, new JournalUsageImpl(System.nanoTime(), 0), _factory, _journalLength, next.getValue1());
    }

    protected void acceptWritable(final WriterBinJournal writable) {

        final ImmutableRangeMap.Builder<Integer, BinJournal> builder = ImmutableRangeMap.builder();

        for(Map.Entry<Range<Integer>, BinJournal> entry : _navigableJournals.asMapOfRanges().entrySet()) {

            builder.put(entry.getKey(), entry.getValue());
        }

        builder.put(writable.range(), writable);

        _navigableJournals = builder.build();

        _journaling = writable;

        LOG.info(String.format("[storage] new writable created, navigableJournals updated:%s", _navigableJournals));
    }

    protected BinJournal downgrade(final BinJournal journal) throws FileNotFoundException {

        Preconditions.checkArgument(journal != null && journal.currentState().isMemoryMapped());

        //this doesn't happen immediately! some journals might still be using the buffer
        //it only takes effect after the journal using the buffer gets garbage collected
        _manager.freeUpBuffer(journal.getMemoryMappedBuffer());

        LOG.info(String.format("[storage] downgrade journal:%s to FileChannelBinJournal", journal));

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

        final ByteBufferBinJournal immutable = new ByteBufferBinJournal(previous.local(),
                BinJournal.JournalState.BufferedReadOnly,
                previous.range(),
                _manager,
                new JournalUsageImpl(previous.usage().getLastModified(), previous.getDocumentSize()),
                _factory,
                previous.getJournalLength(),
                previous.getMemoryMappedBuffer());

        immutable._size = previous.getDocumentSize();
        //assume everything still in use
        immutable.usage().getAlives().set(0, immutable._size);

        return immutable;
    }

    @Subscribe
    public void postExpansion(final PostExpansionEvent event) {

        LOG.info("[storage] post expansion handling");

        final BinJournal previous = event.getPreviousWritable();
        //lots of work here:
        //downgrade previous writable
        //inspect journals to update usages
        //determine if compression must be triggered
        //determine if eviction must be triggered
        Preconditions.checkArgument(previous.currentState().isWritable());

        final ImmutableRangeMap.Builder<Integer, BinJournal> builder = ImmutableRangeMap.builder();

        final Map<Range<Integer>, BinJournal> memoryMappedJournals = Maps.newLinkedHashMap();
        //iterate by last modified
        for(BinJournal journal : this) {
            if(!Objects.equal(previous.range(), journal.range())){
                if(journal.currentState().isMemoryMapped()){
                    memoryMappedJournals.put(journal.range(), journal);
                    continue;
                }
                builder.put(journal.range(), journal);
            }
        }

        //check for downgrades
        int downgrades = memoryMappedJournals.size() - _maxMemoryMappedJournals + 1;
        LOG.info(String.format("[storage] downgrades:%d memoryMappedJournals", downgrades));
        for(Iterator<Map.Entry<Range<Integer>, BinJournal>> it = memoryMappedJournals.entrySet().iterator(); it.hasNext() && downgrades > 0; downgrades -= 1){

            final Map.Entry<Range<Integer>, BinJournal> entry = it.next();
            try {
                builder.put(entry.getKey(), downgrade(entry.getValue()));
            }
            catch (FileNotFoundException e) {
                LOG.warning(Throwables.getStackTraceAsString(e));
            }
        }

        //make previous writable readonly
        LOG.info("[storage] make previous writable immutable");
        builder.put(previous.range(), immutable(previous));

        //keep writable in it
        builder.put(_journaling.range(), _journaling);

        _navigableJournals = builder.build();

        if(_openInvestigation == null){
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
                                LOG.info(String.format("[storage] forced out aging journal:%s", journal));
                                investigation.put(InspectionReport.EvictionRequired, journal);
                                continue;
                            }

                            LOG.info(String.format("[storage] make investigation on aging journal:%s", journal));

                            final JournalUsage usage = journal.usage();
                            final SparseBitSet alives = usage.getAlives();
                            int index = 0;

                            for(Iterator<BinDocument> it = journal.iterator(); it.hasNext(); index += 1){
                                final BinDocument suspect = it.next();
                                if(alives.get(index)){
                                    alives.set(index, _usageTrack.using(suspect));
                                }
                                if(alives.cardinality() > journal.getDocumentSize() * 0.1f){
                                    break;//no more investigation needed upon this journal till suspect time.
                                }
                            }

                            if(usage.isAllDead()){
                                investigation.put(InspectionReport.EvictionRequired, journal);
                            }
                            else if(usage.getUsageRatio() < 0.1f){
                                investigation.put(InspectionReport.CompressionRequired, journal);
                            }
                            else{
                                investigation.put(InspectionReport.RemainAsIs, journal);
                            }
                        }
                    }

                    LOG.info(String.format("[storage] investigation completed:%s", investigation));

                    _eventBus.post(new PostInvestigationEvent(investigation));
                }
            });
        }
    }

    @Subscribe
    public void onPostInvestigation(final PostInvestigationEvent event){
        try {
            final ListMultimap<InspectionReport, BinJournal> report = event.getSource();

            LOG.info(String.format("[storage] investigation report:%s to be handled", report));
            final Set<BinJournal> exclusions = Sets.newIdentityHashSet();
            for(BinJournal journal : report.get(InspectionReport.EvictionRequired)){
                for(BinDocument evict: journal){
                    if(_usageTrack.using(evict)){
                        _usageTrack.forget(evict, RemovalCause.SIZE);
                    }
                }
                exclusions.add(journal);
            }

            for(BinJournal journal : report.get(InspectionReport.CompressionRequired)){
                for(BinDocument compress: journal){
                    if(_usageTrack.using(compress)){
                        _usageTrack.refresh(compress);
                    }
                }
                exclusions.add(journal);
            }

            _openInvestigation = null;

            _lock.lock();

            final ImmutableRangeMap.Builder<Integer, BinJournal> builder = ImmutableRangeMap.builder();

            for(Map.Entry<Range<Integer>, BinJournal> entry : _navigableJournals.asMapOfRanges().entrySet()) {
                if(!exclusions.contains(entry.getValue())){
                    builder.put(entry.getKey(), entry.getValue());
                }
            }

            _navigableJournals = builder.build();

        }
        catch(Exception ex){
            LOG.warning(Throwables.getStackTraceAsString(ex));
        }
        finally {
            _lock.unlock();
        }
    }

}
