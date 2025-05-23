/*-
 * #%L
 * Mars N5 source and reader implementations.
 * %%
 * Copyright (C) 2023 - 2025 Karl Duderstadt
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

import java.util.function.Supplier;

import bdv.util.AbstractSource;
import bdv.util.volatiles.SharedQueue;
import bdv.util.volatiles.VolatileViews;
import mpicbg.spim.data.sequence.VoxelDimensions;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.Volatile;
import net.imglib2.cache.volatiles.CacheHints;
import net.imglib2.cache.volatiles.LoadingStrategy;
import net.imglib2.realtransform.AffineTransform3D;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.util.ConstantUtils;
import net.imglib2.FinalInterval;

public class MarsN5VolatileSource<T extends NumericType<T>, V extends Volatile<T> & NumericType<V>>
	extends AbstractSource<V>
{

	private final MarsN5Source<T> source;
	private RandomAccessibleInterval<V> volatileZerosRAI;
	private SharedQueue queue;

	public MarsN5VolatileSource(final MarsN5Source<T> source, final V type,
		final SharedQueue queue)
	{
		super(type, source.getName());
		this.source = source;
		this.queue = queue;
	}

	public MarsN5VolatileSource(final MarsN5Source<T> source,
		final Supplier<V> typeSupplier, final SharedQueue queue)
	{
		this(source, typeSupplier.get(), queue);
	}

	@Override
	public RandomAccessibleInterval<V> getSource(final int t, final int level) {
		// Get the source from MarsN5Source
		RandomAccessibleInterval<T> rai = source.getSource(t, level);

		if (!source.timePointExists(t, level)) {
			// Create or reuse the volatile zeros RAI
			if (volatileZerosRAI == null || !dimensionsMatch(volatileZerosRAI, rai)) {
				V vType = getType();
				vType.setZero();
				vType.setValid(true); // Mark the volatile type as valid

				FinalInterval interval = new FinalInterval(rai);
				volatileZerosRAI = ConstantUtils.constantRandomAccessibleInterval(vType, interval);
			}
			return volatileZerosRAI;
		}

		// For normal cases, wrap as volatile as before
		return VolatileViews.wrapAsVolatile(rai, queue,
				new CacheHints(LoadingStrategy.VOLATILE, level, true));
	}

	// Helper method to check if two RAIs have the same dimensions
	private boolean dimensionsMatch(RandomAccessibleInterval<?> rai1, RandomAccessibleInterval<?> rai2) {
		if (rai1.numDimensions() != rai2.numDimensions()) return false;

		for (int d = 0; d < rai1.numDimensions(); d++) {
			if (rai1.dimension(d) != rai2.dimension(d)) return false;
		}

		return true;
	}

	@Override
	public synchronized void getSourceTransform(final int t, final int level,
		final AffineTransform3D transform)
	{
		source.getSourceTransform(t, level, transform);
	}

	@Override
	public VoxelDimensions getVoxelDimensions() {
		return source.getVoxelDimensions();
	}

	@Override
	public int getNumMipmapLevels() {
		return source.getNumMipmapLevels();
	}
}
