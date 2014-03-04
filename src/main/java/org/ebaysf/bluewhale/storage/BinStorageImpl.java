package org.ebaysf.bluewhale.storage;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.cache.RemovalCause;
import com.google.common.collect.*;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.brettw.SparseBitSet;
import org.ebaysf.bluewhale.document.BinDocument;
import org.ebaysf.bluewhale.document.BinDocumentFactory;
import org.ebaysf.bluewhale.event.PostExpansionEvent;
import org.ebaysf.bluewhale.util.Files;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.Callable;
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
    public static enum InspectionReport {
        EvictionRequired,
        CompressionRequired,
        RemainAsIs
    }

    private static final Logger LOG = Logger.getLogger(BinStorageImpl.class.getName());

    private final File _local;
    private final JournalsManager _manager;
    private final BinDocumentFactory _factory;
    private final int _journalLength;
    private final int _maxJournals;
    private final int _maxMemoryMappedJournals;
    private final EventBus _eventBus;
    private final ListeningExecutorService _executor;
    private final UsageTrack _usageTrack;
    private final ReentrantLock _lock;

    protected volatile BinJournal _journaling;
    protected volatile RangeMap<Integer, BinJournal> _navigableJournals;

    public BinStorageImpl(final File local,
                          final BinDocumentFactory factory,
                          final int journalLength,
                          final int maxJournals,
                          final int maxMemoryMappedJournals,
                          final List<BinJournal> loadings,
                          final EventBus eventBus,
                          final ListeningExecutorService executor,
                          final UsageTrack usageTrack) throws IOException {

        _local = Objects.firstNonNull(local, com.google.common.io.Files.createTempDir());
        _factory = Preconditions.checkNotNull(factory);
        _eventBus = Preconditions.checkNotNull(eventBus);
        _executor = Preconditions.checkNotNull(executor);
        _usageTrack = Preconditions.checkNotNull(usageTrack);

        Preconditions.checkArgument(journalLength > 0);
        Preconditions.checkArgument(maxJournals > 1);
        Preconditions.checkArgument(maxMemoryMappedJournals > 1 && maxMemoryMappedJournals < maxJournals);

        _manager = new JournalsManager(_local, _eventBus, _executor);
        _journalLength = journalLength;
        _maxJournals = maxJournals;
        _maxMemoryMappedJournals = maxMemoryMappedJournals;
        _navigableJournals = ImmutableRangeMap.of();

        _eventBus.register(this);

        _lock = new ReentrantLock(true);

        warmUp(loadings);
        acceptWritable(nextWritable(_local));
    }

    @Override
    public File local() {

        return _local;
    }

    @Override
    public long append(BinDocument binDocument) throws IOException {

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

    @Override
    public BinDocument read(final long token) throws IOException {

        if(token < 0) {
            return null;
        }

        final BinJournal zone = route(token);

        final int offset = (int)(token & MASK_OFFSET);

        return zone.read(offset);
    }

    @Override
    public BinJournal route(final long token) {

        final int journalCode = (int)(token >> Integer.SIZE);

        return _navigableJournals.get(journalCode);
    }

    @Override
    public UsageTrack getUsageTrack() {

        return _usageTrack;
    }

    @Override
    public int getJournalLength() {

        return _journalLength;
    }

    @Override
    public int getMaxJournals() {

        return _maxJournals;
    }

    @Override
    public int getMaxMemoryMappedJournals(){
        return _maxMemoryMappedJournals;
    }

    @Override
    public int getEvictedJournals() {

        return Collections2.filter(_navigableJournals.asMapOfRanges().values(), JOURNAL_EVICTED).size();
    }

    @Override
    public Iterator<BinJournal> iterator() {

        final List<BinJournal> orderedByLastModified = Lists.newArrayList(_navigableJournals.asMapOfRanges().values());

        Collections.sort(orderedByLastModified, new Comparator<BinJournal>() {
            @Override
            public int compare(final BinJournal journalOne, final BinJournal journalTwo) {

                return (int)(journalOne.usage().getLastModified() - journalTwo.usage().getLastModified());
            }
        });

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

        final File next = Files.newJournalFile(dir);

        return new WriterBinJournal(next, range, _manager, _factory, _journalLength, com.google.common.io.Files.map(next, FileChannel.MapMode.READ_WRITE, _journalLength));
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

    protected EventBus getEventBus() {

        return _eventBus;
    }

    protected ListeningExecutorService getExecutor() {

        return _executor;
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
                _factory,
                journal.getJournalLength(),
                journal.getDocumentSize(),
                -1);
    }

    @Subscribe
    protected void postExpansion(final PostExpansionEvent event) {

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
                builder.put(journal.range(), journal);
            }
            if(journal.currentState().isMemoryMapped()){
                memoryMappedJournals.put(journal.range(), journal);
            }
        }

        //check for downgrades
        int downgrades = memoryMappedJournals.size() - _maxMemoryMappedJournals + 1;
        for(Iterator<Map.Entry<Range<Integer>, BinJournal>> it = memoryMappedJournals.entrySet().iterator(); it.hasNext() && downgrades > 0;){

            final Map.Entry<Range<Integer>, BinJournal> entry = it.next();
            try {
                builder.put(entry.getKey(), downgrade(entry.getValue()));
            }
            catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }

        //make previous writable readonly
        LOG.info("[storage] make previous writable immutable");
        builder.put(previous.range(), new ByteBufferBinJournal(previous.local(),
                BinJournal.JournalState.BufferedReadOnly,
                previous.range(),
                _manager,
                _factory,
                previous.getJournalLength(),
                previous.getMemoryMappedBuffer()));

        _navigableJournals = builder.build();

        final ListenableFuture<ListMultimap<InspectionReport, BinJournal>> inspected = _executor.submit(new Callable<ListMultimap<InspectionReport, BinJournal>>() {
            @Override
            public ListMultimap<InspectionReport, BinJournal> call() throws Exception {

                final ListMultimap<InspectionReport, BinJournal> investigation = Multimaps.newListMultimap(
                        Maps.<InspectionReport, Collection<BinJournal>>newHashMap(),
                        new Supplier<List<BinJournal>>() {
                            @Override
                            public List<BinJournal> get() {
                                return Lists.newLinkedList();
                            }
                        });

                for(BinJournal journal : BinStorageImpl.this){

                    if(!journal.currentState().isMemoryMapped()){

                        LOG.info(String.format("[storage] make investigation on aging journal:%s", journal));

                        final JournalUsage usage = journal.usage();
                        final SparseBitSet alives = usage.getAlives();
                        int index = 0;

                        for(Iterator<BinDocument> it = journal.iterator(); it.hasNext(); index += 1){
                            if(alives.get(index)){
                                alives.set(index, _usageTrack.using(it.next()));
                            }
                            if(alives.cardinality() > journal.getDocumentSize() * 0.1f){
                                break;//no more investigation needed upon this journal till next time.
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

                return investigation;
            }
        });

        inspected.addListener(new Runnable() {
            @Override
            public void run() {
                try {
                    final ListMultimap<InspectionReport, BinJournal> report = inspected.get();

                    LOG.info(String.format("[storage] investigation report:%s to be handled", report));
                    for(BinJournal journal : report.get(InspectionReport.EvictionRequired)){
                        for(BinDocument evict: journal){
                            if(_usageTrack.using(evict)){
                                _usageTrack.forget(evict, RemovalCause.SIZE);
                            }
                        }
                    }

                    for(BinJournal journal : report.get(InspectionReport.CompressionRequired)){
                        for(BinDocument compress: journal){
                            if(_usageTrack.using(compress)){
                                _usageTrack.refresh(compress);
                            }
                        }
                    }
                }
                catch(Exception ex){

                }
            }
        }, _executor);
    }

}
