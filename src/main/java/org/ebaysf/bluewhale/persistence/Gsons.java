package org.ebaysf.bluewhale.persistence;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import org.brettw.SparseBitSet;
import org.ebaysf.bluewhale.Cache;
import org.ebaysf.bluewhale.configurable.CacheBuilder;
import org.ebaysf.bluewhale.configurable.Configuration;
import org.ebaysf.bluewhale.configurable.EvictionStrategy;
import org.ebaysf.bluewhale.segment.Segment;
import org.ebaysf.bluewhale.storage.BinJournal;
import org.ebaysf.bluewhale.storage.JournalUsage;
import org.ebaysf.bluewhale.storage.JournalUsageImpl;
import org.ebaysf.bluewhale.util.Files;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by huzhou on 3/6/14.
 */
public abstract class Gsons {

    private static final Logger LOG = LoggerFactory.getLogger(Gsons.class);

    private static final GsonBuilder GSON_BLDR = new GsonBuilder()
            .serializeNulls()
            .excludeFieldsWithModifiers(Modifier.TRANSIENT, Modifier.STATIC);

    public static final Type TTL_TYPE = new TypeToken<Pair<Long, TimeUnit>>(){}.getType();

    private static final Pattern TTL_PATTERN = Pattern.compile("([0-9]+)ns");

    static {
        GSON_BLDR.registerTypeAdapter(File.class, new JsonSerializer<File>() {

            public @Override JsonElement serialize(final File file,
                                                   final Type type,
                                                   final JsonSerializationContext ctx) {

                return new JsonPrimitive(file.getAbsolutePath());
            }
        });

        GSON_BLDR.registerTypeAdapter(File.class, new JsonDeserializer<File>() {

            public @Override File deserialize(final JsonElement json,
                                    final Type typeOfT,
                                    final JsonDeserializationContext context) throws JsonParseException {

                return new File(json.getAsString());
            }
        });

        GSON_BLDR.registerTypeAdapter(TTL_TYPE, new JsonSerializer<Pair<Long, TimeUnit>>() {

            public @Override JsonElement serialize(final Pair<Long, TimeUnit> ttl,
                                                   final Type type,
                                                   final JsonSerializationContext ctx) {

                return new JsonPrimitive(ttl.getValue1().toNanos(ttl.getValue0()) + "ns");
            }
        });

        GSON_BLDR.registerTypeAdapter(TTL_TYPE, new JsonDeserializer<Pair<Long, TimeUnit>>() {

            public @Override Pair<Long, TimeUnit> deserialize(final JsonElement json,
                                                              final Type typeOfT,
                                                              final JsonDeserializationContext context) throws JsonParseException {

                final String nsAsStr = json.getAsJsonPrimitive().getAsString();
                final Matcher ttlMatcher = TTL_PATTERN.matcher(nsAsStr);
                if(ttlMatcher.matches()){
                    return Pair.with(Long.valueOf(ttlMatcher.group(1)), TimeUnit.NANOSECONDS);
                }
                return null;
            }
        });

        GSON_BLDR.registerTypeAdapter(Range.class, new JsonSerializer<Range<Integer>>() {

            public @Override JsonElement serialize(final Range<Integer> range,
                                                   final Type type,
                                                   final JsonSerializationContext ctx) {

                final JsonArray arrayOfRange = new JsonArray();
                arrayOfRange.add(new JsonPrimitive(range.lowerEndpoint().intValue()));
                arrayOfRange.add(new JsonPrimitive(range.upperEndpoint().intValue()));
                return arrayOfRange;
            }
        });

        GSON_BLDR.registerTypeAdapter(Range.class, new JsonDeserializer<Range<Integer>>() {

            public @Override Range<Integer> deserialize(final JsonElement jsonElement,
                                                        final Type type,
                                                        final JsonDeserializationContext ctx) throws JsonParseException {

                final JsonArray arrayOfRange = jsonElement.getAsJsonArray();
                final int lower = arrayOfRange.get(0).getAsInt();
                final int upper = arrayOfRange.get(1).getAsInt();
                return Range.closed(lower, upper);
            }
        });

        GSON_BLDR.registerTypeAdapter(Segment.class, new JsonDeserializer<Segment>() {

            public @Override Segment deserialize(final JsonElement jsonElement,
                                                 final Type type,
                                                 final JsonDeserializationContext ctx) throws JsonParseException {

                final JsonObject asObj = jsonElement.getAsJsonObject();

                final File local = ctx.deserialize(asObj.get("_local"), File.class);
                final Range<Integer> range = ctx.deserialize(asObj.get("_range"), Range.class);
                final Integer size = ctx.deserialize(asObj.get("_size"), Integer.class);

                return new PersistedSegment(local, range, size);
            }
        });

        GSON_BLDR.registerTypeAdapter(RangeMap.class, new JsonSerializer<RangeMap<Integer, Object>>() {

            public @Override JsonElement serialize(final RangeMap<Integer, Object> rangeMap,
                                                   final Type type,
                                                   final JsonSerializationContext ctx) {

                final JsonArray arrayOfRange = new JsonArray();

                for(Object rangedObject : rangeMap.asMapOfRanges().values()){

                    arrayOfRange.add(ctx.serialize(rangedObject));
                }

                return arrayOfRange;
            }
        });

        GSON_BLDR.registerTypeAdapter(JournalUsage.class, new JsonSerializer<JournalUsage>() {

            public @Override JsonElement serialize(final JournalUsage source,
                                                   final Type typeOfSrc,
                                                   final JsonSerializationContext ctx) {

                final JsonObject obj = new JsonObject();
                obj.add("_lastModified", new JsonPrimitive(source.getLastModified()));
                obj.add("_documents", new JsonPrimitive(source.getDocuments()));
                obj.add("_alives", ctx.serialize(source.getAlives()));

                return obj;
            }
        });

        GSON_BLDR.registerTypeAdapter(JournalUsage.class, new JsonDeserializer<JournalUsage>() {

            public @Override JournalUsage deserialize(final JsonElement jsonElement,
                                                      final Type typeOfT,
                                                      final JsonDeserializationContext ctx) throws JsonParseException {

                final JsonObject asObj = jsonElement.getAsJsonObject();

                final long lastModified = asObj.get("_lastModified").getAsLong();
                final int documents = asObj.get("_documents").getAsInt();
                final SparseBitSet alives = ctx.deserialize(asObj.get("_alives"), SparseBitSet.class);

                return new JournalUsageImpl(lastModified, documents, alives);
            }
        });

        GSON_BLDR.registerTypeAdapter(BinJournal.class, new JsonSerializer<BinJournal>() {

            public @Override JsonElement serialize(final BinJournal source,
                                                   final Type typeOfSrc,
                                                   final JsonSerializationContext ctx) {

                final JsonObject obj = new JsonObject();
                obj.add("_local", ctx.serialize(source.local()));
                obj.add("_state", ctx.serialize(source.currentState()));
                obj.add("_journalRange", ctx.serialize(source.range()));
                obj.add("_journalUsage", ctx.serialize(source.usage()));
                obj.add("_length", new JsonPrimitive(source.getJournalLength()));
                obj.add("_size", new JsonPrimitive(source.getDocumentSize()));

                return obj;
            }
        });

        GSON_BLDR.registerTypeAdapter(BinJournal.class, new JsonDeserializer<BinJournal>() {

            public @Override BinJournal deserialize(final JsonElement jsonElement,
                                                    final Type type,
                                                    final JsonDeserializationContext ctx) throws JsonParseException {

                final JsonObject asObj = jsonElement.getAsJsonObject();

                final File local = ctx.deserialize(asObj.get("_local"), File.class);
                final BinJournal.JournalState state = ctx.deserialize(asObj.get("_state"), BinJournal.JournalState.class);
                final Range<Integer> range = ctx.deserialize(asObj.get("_journalRange"), Range.class);
                final JournalUsage usage = ctx.deserialize(asObj.get("_journalUsage"), JournalUsage.class);
                final Integer length = ctx.deserialize(asObj.get("_length"), Integer.class);
                final Integer size = ctx.deserialize(asObj.get("_size"), Integer.class);

                return new PersistedJournal(local, state, range, usage, length, size);
            }
        });

        GSON_BLDR.registerTypeAdapter(Configuration.class, new JsonSerializer<Configuration>() {

            public @Override JsonElement serialize(final Configuration source,
                                                   final Type typeOfSrc,
                                                   final JsonSerializationContext ctx) {

                final JsonObject obj = new JsonObject();
                obj.add("_local", ctx.serialize(source.getLocal()));
                obj.add("_concurrencyLevel", new JsonPrimitive(source.getConcurrencyLevel()));
                obj.add("_maxSegmentDepth", new JsonPrimitive(source.getMaxSegmentDepth()));
                obj.add("_maxPathDepth", new JsonPrimitive(source.getMaxPathDepth()));
                obj.add("_journalLength", new JsonPrimitive(source.getJournalLength()));
                obj.add("_maxJournals", new JsonPrimitive(source.getMaxJournals()));
                obj.add("_maxMemoryMappedJournals", new JsonPrimitive(source.getMaxMemoryMappedJournals()));
                obj.add("_leastJournalUsageRatio", new JsonPrimitive(source.getLeastJournalUsageRatio()));
                obj.add("_dangerousJournalsRatio", new JsonPrimitive(source.getDangerousJournalsRatio()));
                obj.add("_ttl", ctx.serialize(source.getTTL(), TTL_TYPE));

                return obj;
            }
        });

        GSON_BLDR.registerTypeAdapter(Configuration.class, new JsonDeserializer<Configuration>() {

            public @Override Configuration deserialize(final JsonElement json,
                                                       final Type typeOfT,
                                                       final JsonDeserializationContext ctx) throws JsonParseException {

                final JsonObject asObj = json.getAsJsonObject();
                final File local = ctx.deserialize(asObj.get("_local"), File.class);
                final int concurrencyLevel = asObj.get("_concurrencyLevel").getAsInt();
                final int maxSegmentDepth = asObj.get("_maxSegmentDepth").getAsInt();
                final int maxPathDepth = asObj.get("_maxPathDepth").getAsInt();
                final int journalLength = asObj.get("_journalLength").getAsInt();
                final int maxJournals = asObj.get("_maxJournals").getAsInt();
                final int maxMemoryMappedJournals = asObj.get("_maxMemoryMappedJournals").getAsInt();
                final float leastJournalUsageRatio = asObj.get("_leastJournalUsageRatio").getAsFloat();
                final float dangerousJournalsRatio = asObj.get("_dangerousJournalsRatio").getAsFloat();
                final Pair<Long, TimeUnit> ttl = ctx.deserialize(asObj.get("_ttl"), TTL_TYPE);

                return new CacheBuilder.ConfigurationImpl(local, null, null, concurrencyLevel, maxSegmentDepth, maxPathDepth,
                        null, journalLength, maxJournals, maxMemoryMappedJournals, leastJournalUsageRatio, dangerousJournalsRatio, ttl,
                        true, EvictionStrategy.SILENCE, null, null);
            }
        });
    }

    public static final Gson GSON = GSON_BLDR.create();

    public static <K, V> File persist(final Cache<K, V> cache) throws IOException {

        final File local = Preconditions.checkNotNull(cache.getConfiguration().getLocal());
        final File cold = Files.newCacheFile(local);

        final String json = GSON.toJson(cache);
        LOG.info("persistence result: {}", json);

        com.google.common.io.Files.write(json, cold, Charsets.UTF_8);

        return cold;
    }

    public static <K, V> PersistedCache<K, V> load(final File source) throws IOException {

        Preconditions.checkArgument(source.exists() && source.canRead() && source.isFile());

        return GSON.fromJson(new FileReader(source), PersistedCache.class);
    }
}
