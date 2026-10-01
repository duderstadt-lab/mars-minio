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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.janelia.saalfeldlab.n5.Compression;
import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3DatasetAttributes;

import net.imagej.axis.Axes;
import net.imagej.axis.AxisType;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.basictypeaccess.array.ArrayDataAccess;
import net.imglib2.type.NativeType;
import net.imglib2.view.Views;

/**
 * Writes images as sharded Zarr v3 datasets. N5Utils.saveRegion/saveBlock in
 * n5-imglib2 8.0.0 silently drop chunks when writing sharded datasets, so this
 * class writes chunks directly: all chunks of one shard are collected and
 * handed to {@link N5Writer#writeChunks} together, so each shard (one file or
 * one S3 object) is written exactly once.
 *
 * @author Karl Duderstadt
 */
public final class MarsZarrWriter {

    private MarsZarrWriter() {}

    /** Bytes per pixel of an N5 data type. */
    public static int bytesPerPixel(final DataType type) {
        if (type.equals(DataType.UINT8) || type.equals(DataType.INT8)) return 1;
        if (type.equals(DataType.UINT16) || type.equals(DataType.INT16)) return 2;
        if (type.equals(DataType.UINT32) || type.equals(DataType.INT32) ||
                type.equals(DataType.FLOAT32)) return 4;
        if (type.equals(DataType.UINT64) || type.equals(DataType.INT64) ||
                type.equals(DataType.FLOAT64)) return 8;
        throw new IllegalArgumentException("Unsupported data type " + type);
    }

    /**
     * Chooses a shard size (one shard = one file). Shards are whole multiples of
     * the chunk size. Starting from one chunk, the shard is grown over X, Y, Z
     * and then time (then any other axis except channel) until it spans the full
     * extent or would exceed the target size (uncompressed). The channel axis is
     * never grown, so each channel is stored in its own shard(s).
     *
     * @param dims dataset dimensions
     * @param chunk chunk size
     * @param axes axis type per dimension
     * @param bytesPerPixel bytes per pixel
     * @param targetBytes maximum uncompressed shard size in bytes
     * @return the shard size
     */
    public static int[] chooseShardSize(final long[] dims, final int[] chunk,
            final AxisType[] axes, final int bytesPerPixel, final long targetBytes) {
        final int n = dims.length;
        final int[] shard = chunk.clone();
        final List<Integer> order = new ArrayList<>();
        for (final AxisType type : new AxisType[] {Axes.X, Axes.Y, Axes.Z, Axes.TIME})
            for (int d = 0; d < n; d++)
                if (axes[d] == type) order.add(d);
        for (int d = 0; d < n; d++)
            if (!order.contains(d) && axes[d] != Axes.CHANNEL) order.add(d);

        for (final int d : order) {
            long others = bytesPerPixel;
            for (int e = 0; e < n; e++)
                if (e != d) others *= shard[e];
            final long fullExtent = ((dims[d] + chunk[d] - 1) / chunk[d]) * chunk[d];
            final long fit = (targetBytes / others / chunk[d]) * chunk[d];
            shard[d] = (int) Math.max(chunk[d], Math.min(fullExtent, fit));
        }
        return shard;
    }

    /**
     * Create a sharded Zarr v3 dataset and write the image into it.
     *
     * @param img the image, with its minimum at the origin
     * @param writer a Zarr v3 writer (local or S3)
     * @param dataset the dataset path in the container
     * @param chunk chunk size
     * @param shard shard size, whole multiples of the chunk size
     * @param compression chunk compression
     * @param nThreads number of shards written concurrently. Each in-flight shard
     *          holds up to one shard of pixels in memory.
     */
    public static <T extends NativeType<T>> void saveSharded(
            final RandomAccessibleInterval<T> img, final N5Writer writer,
            final String dataset, final int[] chunk, final int[] shard,
            final Compression compression, final int nThreads)
            throws InterruptedException, ExecutionException {
        final int n = img.numDimensions();
        final long[] dims = img.dimensionsAsLongArray();
        final T type = img.firstElement();
        final DataType dataType = N5Utils.dataType(type);

        final ZarrV3DatasetAttributes attributes = new ZarrV3DatasetAttributes.Builder(
                dims, dataType).blockSize(shard).chunkSize(chunk).compression(compression)
                        .build();
        writer.createDataset(dataset, attributes);

        final long[] shardCounts = new long[n];
        long totalShards = 1;
        for (int d = 0; d < n; d++) {
            shardCounts[d] = (dims[d] + shard[d] - 1) / shard[d];
            totalShards *= shardCounts[d];
        }

        final ExecutorService executor = Executors.newFixedThreadPool(Math.max(1, nThreads));
        try {
            final List<Future<?>> futures = new ArrayList<>();
            for (long s = 0; s < totalShards; s++) {
                final long[] shardPos = new long[n];
                long r = s;
                for (int d = 0; d < n; d++) {
                    shardPos[d] = r % shardCounts[d];
                    r /= shardCounts[d];
                }
                futures.add(executor.submit(() -> writeShard(img, writer, dataset,
                        attributes, dataType, type, chunk, shard, shardPos)));
            }
            for (final Future<?> f : futures)
                f.get();
        }
        finally {
            executor.shutdown();
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <T extends NativeType<T>> void writeShard(
            final RandomAccessibleInterval<T> img, final N5Writer writer,
            final String dataset, final ZarrV3DatasetAttributes attributes,
            final DataType dataType, final T type, final int[] chunk, final int[] shard,
            final long[] shardPos) {
        final int n = chunk.length;
        final long[] dims = img.dimensionsAsLongArray();
        final long[] first = new long[n];
        final long[] count = new long[n];
        long total = 1;
        for (int d = 0; d < n; d++) {
            final long chunksPerShard = shard[d] / chunk[d];
            final long chunksInDim = (dims[d] + chunk[d] - 1) / chunk[d];
            first[d] = shardPos[d] * chunksPerShard;
            count[d] = Math.min(chunksPerShard, chunksInDim - first[d]);
            total *= count[d];
        }

        final List<DataBlock<?>> blocks = new ArrayList<>();
        for (long c = 0; c < total; c++) {
            final long[] gridPos = new long[n];
            final long[] min = new long[n];
            final long[] max = new long[n];
            final int[] size = new int[n];
            long r = c;
            for (int d = 0; d < n; d++) {
                gridPos[d] = first[d] + r % count[d];
                r /= count[d];
                min[d] = gridPos[d] * chunk[d];
                max[d] = Math.min(dims[d], min[d] + chunk[d]) - 1;
                size[d] = (int) (max[d] - min[d] + 1);
            }

            final ArrayImg<T, ?> dst = new ArrayImgFactory<>(type).create(size);
            final var srcCursor = Views.flatIterable(Views.zeroMin(Views.interval(img,
                    min, max))).cursor();
            final var dstCursor = dst.cursor();
            while (srcCursor.hasNext())
                dstCursor.next().set(srcCursor.next());

            final DataBlock block = dataType.createDataBlock(size, gridPos);
            final Object storage = ((ArrayDataAccess<?>) dst.update(null))
                    .getCurrentStorageArray();
            System.arraycopy(storage, 0, block.getData(), 0, java.lang.reflect.Array
                    .getLength(block.getData()));
            blocks.add(block);
        }
        writer.writeChunks(dataset, attributes, blocks.toArray(new DataBlock[0]));
    }
}
