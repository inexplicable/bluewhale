package org.ebaysf.bluewhale.persistence;

import com.google.common.io.Files;
import com.google.gson.Gson;
import org.brettw.SparseBitSet;
import org.javatuples.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * Created by huzhou on 3/7/14.
 */
public class GsonsTest {

    @Test
    public void testGsonWithFiles(){

        final Gson gson = Gsons.GSON;
        Assert.assertNotNull(gson);

        final File origin = Files.createTempDir();
        final String str = gson.toJson(origin);

        Assert.assertNotNull(str);

        final File parsed = gson.fromJson(str, File.class);

        Assert.assertNotNull(parsed);

        Assert.assertEquals(origin.getAbsolutePath(), parsed.getAbsolutePath());

        origin.delete();
    }

    @Test
    public void testGsonWithPairs(){

        final Gson gson = Gsons.GSON;
        Assert.assertNotNull(gson);

        final Pair<Long, TimeUnit> origin = new Pair<Long, TimeUnit>(1L, TimeUnit.HOURS);
        final String str = gson.toJson(origin);

        Assert.assertNotNull(str);

        final Pair<Long, TimeUnit> parsed = gson.fromJson(str, Pair.class);

        Assert.assertNotNull(parsed);

        Assert.assertEquals(origin.getValue1().toNanos(origin.getValue0().longValue()),
                parsed.getValue1().toNanos(parsed.getValue0().longValue()));
    }

    @Test
    public void testGsonWithSparseBitSet(){

        final Gson gson = Gsons.GSON;
        Assert.assertNotNull(gson);

        final SparseBitSet origin = new SparseBitSet();
        for(int i = 0; i < 1000000; i += 3){
            origin.set(i);
        }
        final String str = gson.toJson(origin);

        Assert.assertNotNull(str);

        final SparseBitSet parsed = gson.fromJson(str, SparseBitSet.class);

        Assert.assertNotNull(parsed);

        Assert.assertEquals(origin, parsed);

    }
}
