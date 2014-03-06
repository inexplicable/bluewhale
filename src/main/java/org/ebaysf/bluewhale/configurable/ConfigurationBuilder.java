package org.ebaysf.bluewhale.configurable;

import com.google.common.base.Preconditions;
import com.google.common.eventbus.EventBus;
import org.ebaysf.bluewhale.document.BinDocumentFactories;
import org.ebaysf.bluewhale.document.BinDocumentFactory;
import org.ebaysf.bluewhale.serialization.Serializer;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by huzhou on 3/6/14.
 */
public class ConfigurationBuilder implements Configuration {

    private final File _local;
    private final Serializer<?> _keySerializer;
    private final Serializer<?> _valSerializer;

    private int _concurrencyLevel = 3;
    private int _maxSegmentDepth = 2;

    private BinDocumentFactory _factory = BinDocumentFactories.RAW;
    private int _journalLength = 1 << 29;//512MB
    private int _maxJournals = 8;//4G total
    private int _maxMemoryMappedJournals = 2;//1G RAM
    private boolean _cleanUpOnExit = true;//clean up
    private EventBus _eventBus = new EventBus();//synchronous
    private ExecutorService _executor = Executors.newCachedThreadPool();

    public static <K, V> ConfigurationBuilder builder(final File local,
                                                      final Serializer<K> keySerializer,
                                                      final Serializer<V> valSerializer){

        return new <K, V>ConfigurationBuilder(local, keySerializer, valSerializer);
    }

    private <K, V> ConfigurationBuilder(final File local,
                                        final Serializer<K> keySerializer,
                                        final Serializer<V> valSerializer){
        _local = local;
        _keySerializer = keySerializer;
        _valSerializer = valSerializer;
    }

    public @Override File getLocal(){
        return _local;
    }

    @Override
    public <K> Serializer<K> getKeySerializer(){
        return (Serializer<K>)_keySerializer;
    }

    @Override
    public <V> Serializer<V> getValSerializer(){
        return (Serializer<V>)_valSerializer;
    }

    @Override
    public int getConcurrencyLevel(){
        return _concurrencyLevel;
    }

    public ConfigurationBuilder setConcurrencyLevel(final int concurrencyLevel){
        Preconditions.checkArgument(concurrencyLevel > 0 & concurrencyLevel < 16);
        _concurrencyLevel = concurrencyLevel;
        return this;
    }

    @Override
    public int getMaxSegmentDepth(){
        return _maxSegmentDepth;
    }

    public ConfigurationBuilder setMaxSegmentDepth(final int maxSegmentDepth){
        Preconditions.checkArgument(maxSegmentDepth > 0 && maxSegmentDepth < 16);
        _maxSegmentDepth = maxSegmentDepth;
        return this;
    }

    @Override
    public BinDocumentFactory getBinDocumentFactory(){
        return _factory;
    }

    public ConfigurationBuilder setBinDocumentFactory(final BinDocumentFactory factory){
        _factory = Preconditions.checkNotNull(factory);
        return this;
    }

    @Override
    public int getJournalLength(){
        return _journalLength;
    }

    public ConfigurationBuilder setJournalLength(final int journalLength){
        Preconditions.checkArgument(journalLength > 0 && journalLength < Integer.MAX_VALUE);
        _journalLength = journalLength;
        return this;
    }

    @Override
    public int getMaxJournals(){
        return _maxJournals;
    }

    public ConfigurationBuilder setMaxJournals(final int maxJournals){
        Preconditions.checkArgument(maxJournals > 1 && maxJournals < Integer.MAX_VALUE);
        _maxJournals = maxJournals;
        return this;
    }

    @Override
    public int getMaxMemoryMappedJournals(){
        return _maxMemoryMappedJournals;
    }

    public ConfigurationBuilder setMaxMemoryMappedJournals(final int maxMemoryMappedJournals){
        Preconditions.checkArgument(maxMemoryMappedJournals > 1 && maxMemoryMappedJournals < _maxJournals);
        _maxMemoryMappedJournals = maxMemoryMappedJournals;
        return this;
    }

    @Override
    public boolean isCleanUpOnExit(){
        return _cleanUpOnExit;
    }

    public ConfigurationBuilder setCleanUpOnExit(final boolean cleanUpOnExit){
        _cleanUpOnExit = cleanUpOnExit;
        return this;
    }

    @Override
    public EventBus getEventBus(){
        return _eventBus;
    }

    public ConfigurationBuilder setEventBus(final EventBus eventBus){
        _eventBus = Preconditions.checkNotNull(eventBus);
        return this;
    }

    @Override
    public ExecutorService getExecutor(){
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
                getBinDocumentFactory(),
                getJournalLength(),
                getMaxJournals(),
                getMaxMemoryMappedJournals(),
                isCleanUpOnExit(),
                getEventBus(),
                getExecutor());
    }

    protected static class ConfigurationImpl implements Configuration {

        private final File _local;
        private final Serializer<?> _keySerializer;
        private final Serializer<?> _valSerializer;

        private final int _concurrencyLevel;
        private final int _maxSegmentDepth;

        private final BinDocumentFactory _factory;
        private final int _journalLength;//512MB
        private final int _maxJournals;//4G total
        private final int _maxMemoryMappedJournals;//1G RAM
        private final boolean _cleanUpOnExit;//clean up
        private final EventBus _eventBus;//synchronous
        private final ExecutorService _executor;

        public <K, V> ConfigurationImpl(final File local,
                                        final Serializer<K> keySerializer,
                                        final Serializer<V> valSerializer,
                                        final int concurrencyLevel,
                                        final int maxSegmentDepth,
                                        final BinDocumentFactory factory,
                                        final int journalLength,
                                        final int maxJournals,
                                        final int maxMemoryMappedJournals,
                                        final boolean cleanUpOnExit,
                                        final EventBus eventBus,
                                        final ExecutorService executor) {

            _local = local;
            _keySerializer = keySerializer;
            _valSerializer = valSerializer;

            _concurrencyLevel = concurrencyLevel;
            _maxSegmentDepth = maxSegmentDepth;
            _factory = factory;
            _journalLength = journalLength;
            _maxJournals = maxJournals;
            _maxMemoryMappedJournals = maxMemoryMappedJournals;
            _cleanUpOnExit = cleanUpOnExit;
            _eventBus = eventBus;
            _executor = executor;
        }

        public @Override File getLocal(){
            return _local;
        }

        @Override
        public <K> Serializer<K> getKeySerializer(){
            return (Serializer<K>)_keySerializer;
        }

        @Override
        public <V> Serializer<V> getValSerializer(){
            return (Serializer<V>)_valSerializer;
        }

        @Override
        public int getConcurrencyLevel(){
            return _concurrencyLevel;
        }

        @Override
        public int getMaxSegmentDepth(){
            return _maxSegmentDepth;
        }

        @Override
        public BinDocumentFactory getBinDocumentFactory(){
            return _factory;
        }

        @Override
        public int getJournalLength(){
            return _journalLength;
        }

        @Override
        public int getMaxJournals(){
            return _maxJournals;
        }

        @Override
        public int getMaxMemoryMappedJournals(){
            return _maxMemoryMappedJournals;
        }

        @Override
        public boolean isCleanUpOnExit(){
            return _cleanUpOnExit;
        }

        @Override
        public EventBus getEventBus(){
            return _eventBus;
        }

        @Override
        public ExecutorService getExecutor(){
            return _executor;
        }
    }
}

