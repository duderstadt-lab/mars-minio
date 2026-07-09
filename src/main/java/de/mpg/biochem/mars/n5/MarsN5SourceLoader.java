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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;

import bdv.util.volatiles.SharedQueue;
import bdv.viewer.Source;
import de.mpg.biochem.mars.metadata.MarsBdvSource;
import de.mpg.biochem.mars.metadata.MarsMetadata;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.util.Util;
import net.imglib2.view.Views;

/**
 * Turns a {@link MarsBdvSource} N5 dataset reference into a BigDataViewer
 * {@link Source}. Provides both a non-volatile loader (every pixel present on
 * read, required for correct pixel integration / kymograph extraction) and a
 * volatile loader (progressive, on-demand loading, used by the interactive
 * viewer for performance). The volatile/non-volatile choice belongs to the
 * caller: code that scans every pixel (integration, kymograph export) must
 * use {@link #loadN5Source} so no tile is missed; the interactive viewer uses
 * {@link #loadN5VolatileSource}.
 * <p>
 * Reader and dimension caches are instance-level: create one loader per
 * logical scope (a viewer frame, a batch export, a command run) and let it be
 * garbage collected, or call {@link #close()}, to release the N5 readers.
 *
 * @author Karl Duderstadt
 */
public class MarsN5SourceLoader {

	private final Map<String, N5Reader> n5Readers = new HashMap<>();

	// metaUID -> (sourceName -> dimensions)
	private final Map<String, Map<String, long[]>> sourceDimensions =
		new HashMap<>();

	// Largest time dimension seen across sources loaded by this instance.
	private int numTimePoints = 1;

	/** Non-volatile load: all pixels present on read. Use for pixel scanning. */
	public <T extends NumericType<T> & NativeType<T>> Source<T> loadN5Source(
		final MarsBdvSource source, final MarsMetadata meta) throws IOException
	{
		return build(source, meta, false, null);
	}

	/** Volatile load: progressive on-demand loading. Use for the viewer only. */
	public <T extends NumericType<T> & NativeType<T>> Source<T>
		loadN5VolatileSource(final MarsBdvSource source, final MarsMetadata meta,
			final SharedQueue sharedQueue) throws IOException
	{
		return build(source, meta, true, sharedQueue);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private <T extends NumericType<T> & NativeType<T>> Source<T> build(
		final MarsBdvSource source, final MarsMetadata meta,
		final boolean volatileLoad, final SharedQueue sharedQueue)
		throws IOException
	{
		final N5Reader reader;
		if (n5Readers.containsKey(source.getPath())) {
			reader = n5Readers.get(source.getPath());
		}
		else {
			reader = new MarsN5ViewerReaderFun().apply(source.getPath());
			n5Readers.put(source.getPath(), reader);
		}

		final RandomAccessibleInterval wholeImage = volatileLoad ? N5Utils
			.openVolatile(reader, source.getN5Dataset()) : N5Utils.open(reader,
				source.getN5Dataset());

		// wholeImage should be XYT or XYCT. If XYCT, we hyperSlice to get one
		// channel. XYZCT should also be supported.
		final int dims = wholeImage.numDimensions();
		final long[] dimensions = new long[dims];
		wholeImage.dimensions(dimensions);

		sourceDimensions.computeIfAbsent(meta.getUID(), k -> new HashMap<>()).put(
			source.getName(), dimensions);

		final RandomAccessibleInterval image = (dims > 3) ? Views.hyperSlice(
			wholeImage, wholeImage.numDimensions() - 2, source.getChannel())
			: wholeImage;

		final int tSize = (int) image.dimension(image.numDimensions() - 1);
		if (tSize > numTimePoints) numTimePoints = tSize;

		final RandomAccessibleInterval[] images = new RandomAccessibleInterval[1];
		images[0] = image;

		if (source.getSingleTimePointMode()) {
			final AffineTransform3D[] transforms = new AffineTransform3D[tSize];
			// We don't drift correct single time point overlays.
			// Drift should be corrected against them.
			for (int t = 0; t < tSize; t++)
				transforms[t] = source.getAffineTransform3D();

			final int singleTimePoint = source.getSingleTimePoint();
			final MarsSingleTimePointN5Source<T> n5Source =
				new MarsSingleTimePointN5Source<>((T) Util.getTypeFromInterval(image),
					source.getName(), images, transforms, singleTimePoint);

			return volatileLoad ? (Source<T>) n5Source.asVolatile(sharedQueue)
				: n5Source;
		}
		else {
			final AffineTransform3D[] transforms = new AffineTransform3D[tSize];
			for (int t = 0; t < tSize; t++) {
				if (source.getCorrectDrift()) {
					final double dX = meta.getPlane(0, 0, 0, t).getXDrift();
					final double dY = meta.getPlane(0, 0, 0, t).getYDrift();
					transforms[t] = source.getAffineTransform3D(dX, dY);
				}
				else transforms[t] = source.getAffineTransform3D();
			}

			final MarsN5Source<T> n5Source = new MarsN5Source<>((T) Util
				.getTypeFromInterval(image), source.getName(), images, transforms);

			return volatileLoad ? (Source<T>) n5Source.asVolatile(sharedQueue)
				: n5Source;
		}
	}

	/** Dimensions captured for a loaded source, or null if not loaded. */
	public long[] getDimensions(final String metaUID, final String sourceName) {
		final Map<String, long[]> m = sourceDimensions.get(metaUID);
		return (m == null) ? null : m.get(sourceName);
	}

	/** Largest time dimension across all sources loaded by this instance. */
	public int getNumTimePoints() {
		return numTimePoints;
	}

	/** Closes all cached N5 readers and clears caches. */
	public void close() {
		for (N5Reader reader : n5Readers.values())
			reader.close();
		n5Readers.clear();
		sourceDimensions.clear();
	}
}
