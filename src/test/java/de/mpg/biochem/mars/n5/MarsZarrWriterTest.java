/*-
 * #%L
 * Mars N5 source and reader implementations.
 * %%
 * Copyright (C) 2023 - 2026 Karl Duderstadt
 * %%
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * #L%
 */
package de.mpg.biochem.mars.n5;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.janelia.saalfeldlab.n5.FileSystemKeyValueAccess;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3KeyValueWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.google.gson.GsonBuilder;

import net.imagej.axis.Axes;
import net.imagej.axis.AxisType;
import net.imglib2.RandomAccess;
import net.imglib2.img.array.ArrayImgs;
import net.imglib2.type.numeric.integer.UnsignedShortType;

public class MarsZarrWriterTest {

    private static final AxisType[] XYZCT = {Axes.X, Axes.Y, Axes.Z, Axes.CHANNEL,
            Axes.TIME};

    @Test
    void shardSizeGrowsXYZThenTimeAndKeepsChannelSeparate() {
        final long[] dims = {2048, 2048, 20, 3, 500};
        final int[] chunk = {128, 128, 1, 1, 64};
        final int[] shard = MarsZarrWriter.chooseShardSize(dims, chunk, XYZCT, 2,
                512L * 1024 * 1024);
        assertArrayEquals(new int[] {2048, 2048, 1, 1, 64}, new int[] {shard[0],
            shard[1], shard[2] > 1 ? 1 : shard[2], shard[3], shard[4] > 64 ? 64 : shard[4]});
        assertEquals(1, shard[3]);
        long bytes = 2;
        for (final int s : shard) bytes *= s;
        assertTrue(bytes <= 512L * 1024 * 1024);
        for (int d = 0; d < 5; d++) assertEquals(0, shard[d] % chunk[d]);
    }

    @Test
    void smallDatasetIsOneShardPerChannel() {
        final long[] dims = {300, 200, 6, 2, 150};
        final int[] chunk = {128, 128, 1, 1, 64};
        final int[] shard = MarsZarrWriter.chooseShardSize(dims, chunk, XYZCT, 2,
                1024L * 1024 * 1024);
        assertArrayEquals(new int[] {384, 256, 6, 1, 192}, shard);
    }

    @Test
    void shardedRoundTripWithEdgeChunks(@TempDir final Path dir) throws Exception {
        final long[] dims = {37, 29, 5, 2, 7};
        final int[] chunk = {16, 16, 1, 1, 4};
        final int[] shard = {32, 32, 5, 1, 8};
        int total = 1;
        for (final long d : dims) total *= d;
        final short[] data = new short[total];
        for (int i = 0; i < total; i++) data[i] = (short) (i * 31 + 7);

        final String root = dir.resolve("sharded.zarr").toString();
        try (final N5Writer w = new ZarrV3KeyValueWriter(new FileSystemKeyValueAccess(),
                root, new GsonBuilder(), true)) {
            MarsZarrWriter.saveSharded(ArrayImgs.unsignedShorts(data, dims), w, "Pos0",
                    chunk, shard, new GzipCompression(), 3);
        }

        final N5Reader r = new MarsN5Factory().openReader(root);
        assertTrue(r.getDatasetAttributes("Pos0").isSharded());
        final RandomAccess<UnsignedShortType> ra = N5Utils.<UnsignedShortType>open(r,
                "Pos0").randomAccess();
        final long[] pos = new long[5];
        int idx = 0;
        long bad = 0;
        for (pos[4] = 0; pos[4] < dims[4]; pos[4]++)
            for (pos[3] = 0; pos[3] < dims[3]; pos[3]++)
                for (pos[2] = 0; pos[2] < dims[2]; pos[2]++)
                    for (pos[1] = 0; pos[1] < dims[1]; pos[1]++)
                        for (pos[0] = 0; pos[0] < dims[0]; pos[0]++) {
                            ra.setPosition(pos);
                            if (ra.get().get() != (data[idx++] & 0xffff)) bad++;
                        }
        assertEquals(0, bad);

        // 2 x 1 x 1 x 2 x 1 ... shards: 37/32 -> 2, 29/32 -> 1, 5/5 -> 1, 2/1 -> 2, 7/8 -> 1
        try (Stream<Path> files = Files.walk(dir.resolve("sharded.zarr/Pos0/c"))) {
            assertEquals(4, files.filter(Files::isRegularFile).count());
        }
    }
}
