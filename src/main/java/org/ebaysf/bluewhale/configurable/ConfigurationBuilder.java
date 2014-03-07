package org.ebaysf.bluewhale.configurable;

import com.google.common.base.Preconditions;
import com.google.common.eventbus.EventBus;
import com.google.common.io.Files;
import org.ebaysf.bluewhale.document.BinDocumentFactories;
import org.ebaysf.bluewhale.document.BinDocumentFactory;
import org.ebaysf.bluewhale.serialization.Serializer;
import org.javatuples.Pair;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by huzhou on 3/6/14.
 */
public class ConfigurationBuilder implements Configuration {

    private final Serializer<?> _keySerializer;
    private final Serializer<?> _valSerializer;

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
    private boolean _cleanUpOnExit = true;//clean up
    private EvictionStrategy _evictionStrategy = EvictionStrategy.SILENCE;
    private EventBus _eventBus = new EventBus();//synchronous
    private ExecutorService _executor = Executors.newCachedThreadPool();

    public static <K, V> ConfigurationBuilder builder(final Serializer<K> keySerializer,
                                                      final Serializer<V> valSerializer){

        return new <K, V>ConfigurationBuilder(keySerializer, valSerializer);
    }

    private <K, V> ConfigurationBuilder(final Serializer<K> keySerializer,
                                        final Serializer<V> valSerializer){
        _keySerializer = keySerializer;
        _valSerializer = valSerializer;
    }

    public @Override File getLocal(){
        if(_local == null){
            _local = Files.createTempDir();
        }
        return _local;
    }

    public ConfigurationBuilder setLocal(final File local){
        _local = Preconditions.checkNotNull(local);
        return this;
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

    public ConfigurationBuilder setConcurrencyLevel(final int concurrencyLevel){
        Preconditions.checkArgument(concurrencyLevel > 0 & concurrencyLevel < 16);
        _concurrencyLevel = concurrencyLevel;
        return this;
    }

    public @Override int getMaxSegmentDepth(){
        return _maxSegmentDepth;
    }

    public ConfigurationBuilder setMaxSegmentDepth(final int maxSegmentDepth){
        Preconditions.checkArgument(maxSegmentDepth > 0 && maxSegmentDepth < 16);
        _maxSegmentDepth = maxSegmentDepth;
        return this;
    }

    public @Override int getMaxPathDepth(){
        return _maxPathDepth;
    }

    public ConfigurationBuilder setMaxPathDepth(final int maxPathDepth){
        Preconditions.checkArgument(maxPathDepth > 0);
        _maxPathDepth = maxPathDepth;
        return this;
    }

    public @Override BinDocumentFactory getBinDocumentFactory(){
        return _factory;
    }

    public ConfigurationBuilder setBinDocumentFactory(final BinDocumentFactory factory){
        _factory = Preconditions.checkNotNull(factory);
        return this;
    }

    public @Override int getJournalLength(){
        return _journalLength;
    }

    public ConfigurationBuilder setJournalLength(final int journalLength){
        Preconditions.checkArgument(journalLength > 0 && journalLength < Integer.MAX_VALUE);
        _journalLength = journalLength;
        return this;
    }

    public @Override int getMaxJournals(){
        return _maxJournals;
    }

    public ConfigurationBuilder setMaxJournals(final int maxJournals){
        Preconditions.checkArgument(maxJournals > 1 && maxJournals < Integer.MAX_VALUE);
        _maxJournals = maxJournals;
        return this;
    }

    public @Override int getMaxMemoryMappedJournals(){
        return _maxMemoryMappedJournals;
    }

    public ConfigurationBuilder setMaxMemoryMappedJournals(final int maxMemoryMappedJournals){
        Preconditions.checkArgument(maxMemoryMappedJournals > 1 && maxMemoryMappedJournals < _maxJournals);
        _maxMemoryMappedJournals = maxMemoryMappedJournals;
        return this;
    }

    public @Override float getLeastJournalUsageRatio(){
        return _leastJournalUsageRatio;
    }

    public ConfigurationBuilder setLeastJournalUsageRatio(final float leastJournalUsageRatio){
        Preconditions.checkArgument(leastJournalUsageRatio >= 0f && leastJournalUsageRatio < 0.5f);
        _leastJournalUsageRatio = leastJournalUsageRatio;
        return this;
    }

    public @Override float getDangerousJournalsRatio(){
        return _dangerousJournalsRatio;
    }

    public ConfigurationBuilder setDangerousJournalsRatio(final float dangerousJournalsRatio){
        Preconditions.checkArgument(dangerousJournalsRatio > 0f && dangerousJournalsRatio < 0.5f);
        _dangerousJournalsRatio = dangerousJournalsRatio;
        return this;
    }

    public @Override Pair<Long, TimeUnit> getTTL(){
        return _ttl;
    }

    public ConfigurationBuilder setTTL(final Pair<Long, TimeUnit> ttl){
        _ttl = Preconditions.checkNotNull(ttl);
        return this;
    }

    public @Override boolean isCleanUpOnExit(){
        return _cleanUpOnExit;
    }

    public ConfigurationBuilder setCleanUpOnExit(final boolean cleanUpOnExit){
        _cleanUpOnExit = cleanUpOnExit;
        return this;
    }

    public @Override EvictionStrategy getEvictionStrategy() {
        return _evictionStrategy;
    }

    public ConfigurationBuilder setEvictionStrategy(final EvictionStrategy evictionStrategy){
        _evictionStrategy = Preconditions.checkNotNull(evictionStrategy);
        return this;
    }

    public @Override EventBus getEventBus(){
        return _eventBus;
    }

    public ConfigurationBuilder setEventBus(final EventBus eventBus){
        _eventBus = Preconditions.checkNotNull(eventBus);
        return this;
    }

    public @Override ExecutorService getExecutor(){
        return _executor;
    }

    public ConfigurationBuilder setExecutor(final ExecutorService executor){
        _executor = Preconditions.checkNotNull(executor);
        return this;
    }

    public Configuration build(){
        return new ConfigurationImpl(getLocal(),
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
                isCleanUpOnExit(),
                getEvictionStrategy(),
                getEventBus(),
                getExecutor());
    }

    protected static class ConfigurationImpl implements Configuration {

        private final File _local;
        private final Serializer<?> _keySerializer;
        private final Serializer<?> _valSerializer;

        private final int _concurrencyLevel;
        private final int _maxSegmentDepth;
        private final int _maxPathDepth;

        private final BinDocumentFactory _factory;
        private final int _journalLength;//512MB
        private final int _maxJournals;//4G total
        private final int _maxMemoryMappedJournals;//1G RAM
        private final float _leastJournalUsageRatio;
        private final float _dangerousJournalsRatio;
        private final Pair<Long, TimeUnit> _ttl;
        private final boolean _cleanUpOnExit;//clean up
        private final EvictionStrategy _evictionStrategy;
        private final EventBus _eventBus;//synchronous
        private final ExecutorService _executor;

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
                                        final boolean cleanUpOnExit,
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
            _cleanUpOnExit = cleanUpOnExit;
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

        public @Override boolean isCleanUpOnExit(){
            return _cleanUpOnExit;
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

