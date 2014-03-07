package org.ebaysf.bluewhale.persistence;

import com.google.common.io.BaseEncoding;
import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import org.brettw.SparseBitSet;
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

    private static final GsonBuilder _gsonBuilder = new GsonBuilder().excludeFieldsWithModifiers(Modifier.TRANSIENT);

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

        _gsonBuilder.registerTypeAdapter(_ttlType, new JsonSerializer<Pair<Long, TimeUnit>>() {

            public @Override JsonElement serialize(final Pair<Long, TimeUnit> ttl,
                                                   final Type type,
                                                   final JsonSerializationContext jsonSerializationContext) {

                return new JsonPrimitive(ttl.getValue1().toNanos(ttl.getValue0()) + "ns");
            }
        });

        _gsonBuilder.registerTypeAdapter(_ttlType, new JsonDeserializer<Pair<Long, TimeUnit>>() {

            public @Override Pair<Long, TimeUnit> deserialize(final JsonElement json,
                                                              final Type typeOfT,
                                                              final JsonDeserializationContext context) throws JsonParseException {

                final String nsAsStr = json.getAsJsonPrimitive().getAsString();
                final Matcher ttlMatcher = _ttlPattern.matcher(nsAsStr);
                if(ttlMatcher.matches()){
                    return new Pair<Long, TimeUnit>(Long.valueOf(ttlMatcher.group(0)), TimeUnit.NANOSECONDS);
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
    }
}
