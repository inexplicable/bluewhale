package org.ebaysf.bluewhale.persistence;

import com.google.common.collect.Range;
import com.google.common.io.BaseEncoding;
import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import org.brettw.SparseBitSet;
import org.ebaysf.bluewhale.segment.Segment;
import org.javatuples.Pair;

import java.io.*;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by huzhou on 3/6/14.
 */
public abstract class Gsons {

    private static final GsonBuilder _gsonBuilder = new GsonBuilder()
            .serializeNulls()
            .excludeFieldsWithModifiers(Modifier.TRANSIENT, Modifier.STATIC);

    private static final Type _ttlType = new TypeToken<Pair<Long, TimeUnit>>(){}.getType();
    private static final Pattern _ttlPattern = Pattern.compile("([0-9]+)ns");

    static {
        _gsonBuilder.registerTypeAdapter(File.class, new JsonSerializer<File>() {

            public @Override JsonElement serialize(final File file,
                                                   final Type type,
                                                   final JsonSerializationContext jsonSerializationContext) {

                return new JsonPrimitive(file.getAbsolutePath());
            }
        });

        _gsonBuilder.registerTypeAdapter(File.class, new JsonDeserializer<File>() {

            public @Override File deserialize(final JsonElement json,
                                    final Type typeOfT,
                                    final JsonDeserializationContext context) throws JsonParseException {

                return new File(json.getAsJsonPrimitive().getAsString());
            }
        });

        _gsonBuilder.registerTypeAdapter(Pair.class, new JsonSerializer<Pair<Long, TimeUnit>>() {

            public @Override JsonElement serialize(final Pair<Long, TimeUnit> ttl,
                                                   final Type type,
                                                   final JsonSerializationContext jsonSerializationContext) {

                return new JsonPrimitive(ttl.getValue1().toNanos(ttl.getValue0()) + "ns");
            }
        });

        _gsonBuilder.registerTypeAdapter(Pair.class, new JsonDeserializer<Pair<Long, TimeUnit>>() {

            public @Override Pair<Long, TimeUnit> deserialize(final JsonElement json,
                                                              final Type typeOfT,
                                                              final JsonDeserializationContext context) throws JsonParseException {

                final String nsAsStr = json.getAsJsonPrimitive().getAsString();
                final Matcher ttlMatcher = _ttlPattern.matcher(nsAsStr);
                if(ttlMatcher.matches()){
                    return new Pair<Long, TimeUnit>(Long.valueOf(ttlMatcher.group(1)), TimeUnit.NANOSECONDS);
                }
                return null;
            }
        });

        _gsonBuilder.registerTypeAdapter(SparseBitSet.class, new JsonSerializer<SparseBitSet>() {

            public @Override JsonElement serialize(final SparseBitSet sparseBitSet,
                                                   final Type type,
                                                   final JsonSerializationContext jsonSerializationContext) {

                try{
                    final ByteArrayOutputStream bos = new ByteArrayOutputStream(sparseBitSet.length());
                    final ObjectOutputStream oos = new ObjectOutputStream(bos);
                    oos.writeObject(sparseBitSet);

                    return new JsonPrimitive(BaseEncoding.base64().encode(bos.toByteArray()));
                }
                catch(IOException ex){
                    ex.printStackTrace();
                }
                return null;
            }
        });

        _gsonBuilder.registerTypeAdapter(SparseBitSet.class, new JsonDeserializer<SparseBitSet>() {

            public @Override SparseBitSet deserialize(final JsonElement jsonElement,
                                                      final Type type,
                                                      final JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {

                try{
                    final ByteArrayInputStream bis = new ByteArrayInputStream(BaseEncoding.base64().decode(jsonElement.getAsJsonPrimitive().getAsString()));
                    final ObjectInputStream ois = new ObjectInputStream(bis);

                    return (SparseBitSet)ois.readObject();
                }
                catch(Exception ex){
                    ex.printStackTrace();
                }
                return null;
            }
        });

        _gsonBuilder.registerTypeAdapter(Range.class, new JsonSerializer<Range<Integer>>() {

            public @Override JsonElement serialize(final Range<Integer> range,
                                                   final Type type,
                                                   final JsonSerializationContext jsonSerializationContext) {

                final JsonArray arrayOfRange = new JsonArray();
                arrayOfRange.add(new JsonPrimitive(range.lowerEndpoint().intValue()));
                arrayOfRange.add(new JsonPrimitive(range.upperEndpoint().intValue()));
                return arrayOfRange;
            }
        });

        _gsonBuilder.registerTypeAdapter(Range.class, new JsonDeserializer<Range<Integer>>() {

            public @Override Range<Integer> deserialize(final JsonElement jsonElement,
                                                        final Type type,
                                                        final JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {

                final JsonArray arrayOfRange = jsonElement.getAsJsonArray();
                final int lower = arrayOfRange.get(0).getAsInt();
                final int upper = arrayOfRange.get(1).getAsInt();
                return Range.closed(lower, upper);
            }
        });

        _gsonBuilder.registerTypeAdapter(Segment.class, new JsonDeserializer<Segment>() {

            public @Override Segment deserialize(final JsonElement jsonElement,
                                                 final Type type,
                                                 final JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {

                final File local = jsonDeserializationContext.deserialize(jsonElement.getAsJsonObject().get("_local"), File.class);
                final Range<Integer> range = jsonDeserializationContext.deserialize(jsonElement.getAsJsonObject().get("_range"), Range.class);
                final int size = jsonDeserializationContext.deserialize(jsonElement.getAsJsonObject().get("_size"), Integer.class);

                return new PersistedSegment(local, range, size);
            }
        });
//
//        _gsonBuilder.registerTypeAdapter(Segment.class, new InstanceCreator<Segment>() {
//
//        });
    }

    public static final Gson GSON = _gsonBuilder.create();
}
