package org.ebaysf.bluewhale.configurable;

import com.google.common.base.Preconditions;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.eventbus.EventBus;
import com.google.common.io.Files;
import org.ebaysf.bluewhale.Cache;
import org.ebaysf.bluewhale.CacheImpl;
import org.ebaysf.bluewhale.document.BinDocumentFactories;
import org.ebaysf.bluewhale.document.BinDocumentFactory;
import org.ebaysf.bluewhale.persistence.Gsons;
import org.ebaysf.bluewhale.persistence.PersistedCache;
import org.ebaysf.bluewhale.segment.Segment;
import org.ebaysf.bluewhale.serialization.Serializer;
import org.ebaysf.bluewhale.storage.BinJournal;
import org.javatuples.Pair;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by huzhou on 3/6/14.
 */
public class CacheBuilder<K, V> implements Configuration {

    private final Serializer<K> _keySerializer;
    private final Serializer<V> _valSerializer;
    private RemovalListener<K, V> _removalListener = new RemovalListener<K, V>() {
        public @Override void onRemoval(RemovalNotification<K, V> notification) {

        }
    };

    private File _local;
    private int _concurrencyLevel = 3;
    private int _maxSegmentDepth = 2;
    private int _maxPathDepth = 7;

    private BinDocumentFactory _factory = BinDocumentFactories.RAW;
    private int _journalLength = 1 << 29;//512MB
    private int _maxJournals = 8;//4G total
    private int _maxMemoryMappedJournals = 2;//1G RAM
    private float _leastJournalUsageRatio = 0.1f;
    private float _dangerousJournalsRatio = 0.25f;//1/4
    private Pair<Long, TimeUnit> _ttl = null;
    private boolean _persistent = false;//clean up
    private EvictionStrategy _evictionStrategy = EvictionStrategy.SILENCE;
    private EventBus _eventBus = new EventBus();//synchronous
    private ExecutorService _executor = Executors.newCachedThreadPool();

    private List<Segment> _coldSegments = Collections.emptyList();
    private List<BinJournal> _coldJournals = Collections.emptyList();

    public static <K, V> CacheBuilder builder(final Serializer<K> keySerializer,
                                              final Serializer<V> valSerializer) {

        return new CacheBuilder<K, V>(keySerializer, valSerializer);
    }

    private CacheBuilder(final Serializer<K> keySerializer,
                         final Serializer<V> valSerializer) {

        _keySerializer = keySerializer;
        _valSerializer = valSerializer;
    }

    public @Override File getLocal(){
        if(_local == null){
            _local = Files.createTempDir();
        }
        return _local;
    }

    public CacheBuilder<K, V> local(final File local){
        _local = Preconditions.checkNotNull(local);
        return this;
    }

    public CacheBuilder<K, V> cold(final File source) throws IOException {

        final PersistedCache<K, V> cold = Gsons.load(source);

        final Configuration configuration = cold.getConfiguration();

        this.concurrencyLevel(configuration.getConcurrencyLevel())
            .maxSegmentDepth(configuration.getMaxSegmentDepth())
            .maxPathDepth(configuration.getMaxPathDepth())
            .journalLength(configuration.getJournalLength())
            .maxJournals(configuration.getMaxJournals())
            .maxMemoryMappedJournals(configuration.getMaxMemoryMappedJournals())
            .leastJournalUsageRatio(configuration.getLeastJournalUsageRatio())
            .dangerousJournalsRatio(configuration.getDangerousJournalsRatio())
            .ttl(configuration.getTTL())
            .persists(true);

        _coldSegments = Preconditions.checkNotNull(cold.getPersistedSegments());
        _coldJournals = Preconditions.checkNotNull(cold.getPersistedJournals());

        return this;
    }

    public @Override Serializer<K> getKeySerializer(){
        return _keySerializer;
    }

    public @Override Serializer<V> getValSerializer(){
        return _valSerializer;
    }

    public CacheBuilder<K, V> removalListener(final RemovalListener<K, V> removalListener){
        _removalListener = Preconditions.checkNotNull(removalListener);
        return this;
    }

    public @Override int getConcurrencyLevel(){
        return _concurrencyLevel;
    }

    public CacheBuilder<K, V> concurrencyLevel(final int concurrencyLevel){
        Preconditions.checkArgument(concurrencyLevel > 0 & concurrencyLevel < 16);
        _concurrencyLevel = concurrencyLevel;
        return this;
    }

    public @Override int getMaxSegmentDepth(){
        return _maxSegmentDepth;
    }

    public CacheBuilder<K, V> maxSegmentDepth(final int maxSegmentDepth){
        Preconditions.checkArgument(maxSegmentDepth > 0 && maxSegmentDepth < 16);
        _maxSegmentDepth = maxSegmentDepth;
        return this;
    }

    public @Override int getMaxPathDepth(){
        return _maxPathDepth;
    }

    public CacheBuilder<K, V> maxPathDepth(final int maxPathDepth){
        Preconditions.checkArgument(maxPathDepth > 0);
        _maxPathDepth = maxPathDepth;
        return this;
    }

    public @Override BinDocumentFactory getBinDocumentFactory(){
        return _factory;
    }

    public CacheBuilder<K, V> binDocumentFactory(final BinDocumentFactory factory){
        _factory = Preconditions.checkNotNull(factory);
        return this;
    }

    public @Override int getJournalLength(){
        return _journalLength;
    }

    public CacheBuilder<K, V> journalLength(final int journalLength){
        Preconditions.checkArgument(journalLength > 0 && journalLength < Integer.MAX_VALUE);
        _journalLength = journalLength;
        return this;
    }

    public @Override int getMaxJournals(){
        return _maxJournals;
    }

    public CacheBuilder<K, V> maxJournals(final int maxJournals){
        Preconditions.checkArgument(maxJournals > 1 && maxJournals < Integer.MAX_VALUE);
        _maxJournals = maxJournals;
        return this;
    }

    public @Override int getMaxMemoryMappedJournals(){
        return _maxMemoryMappedJournals;
    }

    public CacheBuilder<K, V> maxMemoryMappedJournals(final int maxMemoryMappedJournals){
        Preconditions.checkArgument(maxMemoryMappedJournals > 1 && maxMemoryMappedJournals < _maxJournals);
        _maxMemoryMappedJournals = maxMemoryMappedJournals;
        return this;
    }

    public @Override float getLeastJournalUsageRatio(){
        return _leastJournalUsageRatio;
    }

    public CacheBuilder<K, V> leastJournalUsageRatio(final float leastJournalUsageRatio){
        Preconditions.checkArgument(leastJournalUsageRatio >= 0f && leastJournalUsageRatio < 0.5f);
        _leastJournalUsageRatio = leastJournalUsageRatio;
        return this;
    }

    public @Override float getDangerousJournalsRatio(){
        return _dangerousJournalsRatio;
    }

    public CacheBuilder<K, V> dangerousJournalsRatio(final float dangerousJournalsRatio){
        Preconditions.checkArgument(dangerousJournalsRatio > 0f && dangerousJournalsRatio < 0.5f);
        _dangerousJournalsRatio = dangerousJournalsRatio;
        return this;
    }

    public @Override Pair<Long, TimeUnit> getTTL(){
        return _ttl;
    }

    public CacheBuilder<K, V> ttl(final Pair<Long, TimeUnit> ttl){
        _ttl = Preconditions.checkNotNull(ttl);
        return this;
    }

    public @Override boolean isPersistent(){
        return _persistent;
    }

    public CacheBuilder<K, V> persists(final boolean persistent){
        _persistent = persistent;
        return this;
    }

    public @Override EvictionStrategy getEvictionStrategy() {
        return _evictionStrategy;
    }

    public CacheBuilder<K, V> evictionStrategy(final EvictionStrategy evictionStrategy){
        _evictionStrategy = Preconditions.checkNotNull(evictionStrategy);
        return this;
    }

    public @Override EventBus getEventBus(){
        return _eventBus;
    }

    public CacheBuilder<K, V> eventBus(final EventBus eventBus){
        _eventBus = Preconditions.checkNotNull(eventBus);
        return this;
    }

    public @Override ExecutorService getExecutor(){
        return _executor;
    }

    public CacheBuilder<K, V> executor(final ExecutorService executor){
        _executor = Preconditions.checkNotNull(executor);
        return this;
    }

    public Cache<K, V> build() throws IOException {

        final Configuration configuration = new ConfigurationImpl(getLocal(),
                getKeySerializer(),
                getValSerializer(),
                getConcurrencyLevel(),
                getMaxSegmentDepth(),
                getMaxPathDepth(),
                getBinDocumentFactory(),
                getJournalLength(),
                getMaxJournals(),
                getMaxMemoryMappedJournals(),
                getLeastJournalUsageRatio(),
                getDangerousJournalsRatio(),
                getTTL(),
                isPersistent(),
                getEvictionStrategy(),
                getEventBus(),
                getExecutor());

        return new CacheImpl<K, V>(configuration, _removalListener, _coldSegments, _coldJournals);
    }

    public static class ConfigurationImpl implements Configuration {

        private final File _local;
        private final transient Serializer<?> _keySerializer;
        private final transient Serializer<?> _valSerializer;

        private final int _concurrencyLevel;
        private final int _maxSegmentDepth;
        private final int _maxPathDepth;

        private final transient BinDocumentFactory _factory;
        private final int _journalLength;//512MB
        private final int _maxJournals;//4G total
        private final int _maxMemoryMappedJournals;//1G RAM
        private final float _leastJournalUsageRatio;
        private final float _dangerousJournalsRatio;
        private final Pair<Long, TimeUnit> _ttl;
        private final boolean _persistent;
        private final transient EvictionStrategy _evictionStrategy;
        private final transient EventBus _eventBus;//synchronous
        private final transient ExecutorService _executor;

        public <K, V> ConfigurationImpl(final File local,
                                        final Serializer<K> keySerializer,
                                        final Serializer<V> valSerializer,
                                        final int concurrencyLevel,
                                        final int maxSegmentDepth,
                                        final int maxPathDepth,
                                        final BinDocumentFactory factory,
                                        final int journalLength,
                                        final int maxJournals,
                                        final int maxMemoryMappedJournals,
                                        final float leastJournalUsageRatio,
                                        final float dangerousJournalsRatio,
                                        final Pair<Long, TimeUnit> ttl,
                                        final boolean persistent,
                                        final EvictionStrategy evictionStrategy,
                                        final EventBus eventBus,
                                        final ExecutorService executor) {

            _local = local;
            _keySerializer = keySerializer;
            _valSerializer = valSerializer;

            _concurrencyLevel = concurrencyLevel;
            _maxSegmentDepth = maxSegmentDepth;
            _maxPathDepth = maxPathDepth;
            _factory = factory;
            _journalLength = journalLength;
            _maxJournals = maxJournals;
            _maxMemoryMappedJournals = maxMemoryMappedJournals;
            _leastJournalUsageRatio = leastJournalUsageRatio;
            _dangerousJournalsRatio = dangerousJournalsRatio;
            _ttl = ttl;
            _persistent = persistent;
            _evictionStrategy = evictionStrategy;
            _eventBus = eventBus;
            _executor = executor;
        }

        public @Override File getLocal(){
            return _local;
        }

        public @Override <K> Serializer<K> getKeySerializer(){
            return (Serializer<K>)_keySerializer;
        }

        public @Override <V> Serializer<V> getValSerializer(){
            return (Serializer<V>)_valSerializer;
        }

        public @Override int getConcurrencyLevel(){
            return _concurrencyLevel;
        }

        public @Override int getMaxSegmentDepth(){
            return _maxSegmentDepth;
        }

        public @Override int getMaxPathDepth(){
            return _maxPathDepth;
        }

        public @Override BinDocumentFactory getBinDocumentFactory(){
            return _factory;
        }

        public @Override int getJournalLength(){
            return _journalLength;
        }

        public @Override int getMaxJournals(){
            return _maxJournals;
        }

        public @Override int getMaxMemoryMappedJournals(){
            return _maxMemoryMappedJournals;
        }

        public @Override float getLeastJournalUsageRatio(){
            return _leastJournalUsageRatio;
        }

        public @Override float getDangerousJournalsRatio(){
            return _dangerousJournalsRatio;
        }

        public @Override Pair<Long, TimeUnit> getTTL(){
            return _ttl;
        }

        public @Override boolean isPersistent(){
            return _persistent;
        }

        public @Override EvictionStrategy getEvictionStrategy(){
            return _evictionStrategy;
        }

        public @Override EventBus getEventBus(){
            return _eventBus;
        }

        public @Override ExecutorService getExecutor(){
            return _executor;
        }
    }
}

