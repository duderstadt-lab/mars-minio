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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.scijava.Context;

import de.mpg.biochem.mars.scifio.MarsMicromanagerFormat;

import io.scif.FormatException;
import io.scif.Metadata;
import io.scif.img.SCIFIOImgPlus;
import net.imagej.Dataset;
import net.imagej.DatasetService;
import net.imagej.ImgPlus;
import net.imagej.axis.Axes;
import net.imagej.axis.AxisType;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.cache.img.CachedCellImg;
import net.imglib2.img.Img;
import net.imglib2.img.planar.PlanarImg;
import net.imglib2.img.planar.PlanarImgFactory;
import net.imglib2.loops.LoopBuilder;
import net.imglib2.parallel.DefaultTaskExecutor;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.NumericType;
import net.imglib2.util.Util;

/**
 * Opens a single dataset inside an N5 container as an ImageJ {@link Dataset}
 * (an ImagePlus once shown through the UI service).
 * <p>
 * This is the shared open path used by both the "Open N5 as ImagePlus" command
 * and the Dataset Explorer's double-click action, so the two cannot drift.
 * <p>
 * <b>Metadata.</b> Mars writes Micromanager acquisitions to N5 with the
 * original {@code metadata.txt} sitting next to the dataset
 * ({@code <root>/<dataset>/metadata.txt}). When that file is present it is
 * parsed with {@link MarsMicromanagerFormat} and attached to the image, so
 * channel names, exposure, timestamps and pixel calibration survive the round
 * trip. When it is absent — an N5 written by anything else — the image still
 * opens, just with default axes and no acquisition metadata.
 * <p>
 * Works for both S3/MinIO roots (canonical {@code scheme://bucket.s3.host/path}
 * URLs, see {@link MarsS3Browser#buildPath}) and plain local {@code .n5}
 * directories.
 *
 * @author Karl Duderstadt
 */
public final class MarsN5ImagePlusOpener {

    /**
     * Axis order Mars N5 volumes are written in. Truncated to the dataset's
     * actual dimensionality — {@link ImgPlus} requires exactly one axis per
     * dimension and throws otherwise, and datasets not written by Mars are
     * frequently 2D or 3D.
     */
    private static final AxisType[] DEFAULT_AXIS_ORDER = { Axes.X, Axes.Y,
            Axes.Z, Axes.CHANNEL, Axes.TIME };

    private static final String METADATA_FILE = "metadata.txt";

    private MarsN5ImagePlusOpener() {}

    /**
     * Open one dataset from an N5 container.
     *
     * @param n5RootUrl the .n5 root — a canonical Mars S3 URL or a local path.
     * @param datasetPath the dataset inside it, e.g. "Pos0" (a leading slash is
     *          accepted, as N5 metadata paths carry one).
     * @param virtual true to leave the image backed by lazily-fetched N5 chunks;
     *          false to copy it into a planar in-memory image up front.
     * @param context the SciJava context supplying the dataset service and the
     *          Micromanager format parser.
     * @return the opened dataset, with its source set to the dataset's URL.
     * @throws IOException if the container or dataset cannot be read.
     */
    public static Dataset open(final String n5RootUrl, final String datasetPath,
            final boolean virtual, final Context context) throws IOException
    {
        final N5Reader n5 = new MarsN5ViewerReaderFun().apply(n5RootUrl);
        if (n5 == null) throw new IOException("Could not open N5 container: "
                + n5RootUrl);
        return open(n5, n5RootUrl, datasetPath, virtual, context);
    }

    /**
     * As {@link #open(String, String, boolean, Context)}, but reusing an N5
     * reader the caller already holds. {@code n5RootUrl} is still required: it
     * locates the sidecar {@code metadata.txt} and names the image source.
     */
    public static Dataset open(final N5Reader n5, final String n5RootUrl,
            final String datasetPath, final boolean virtual,
            final Context context) throws IOException
    {
        final String dataset = normalizeDataset(datasetPath);
        final Metadata metadata = readMicromanagerMetadata(n5RootUrl, dataset,
                context);

        // An empty relative path means the .n5 root is itself the dataset; N5
        // addresses that as "/", and the container's own name is the best label.
        final String n5Path = dataset.isEmpty() ? "/" : dataset;
        final String imageName = dataset.isEmpty() ? rootName(n5RootUrl) : dataset;

        final Dataset out = buildDataset(n5, n5Path, imageName, metadata, virtual,
                context);
        out.setSource(datasetUrl(n5RootUrl, dataset));
        return out;
    }

    /**
     * Read the Micromanager {@code metadata.txt} that sits beside a dataset and
     * parse it into SCIFIO metadata, or return null when there is none (or it
     * cannot be parsed) — in which case the caller opens with defaults.
     */
    public static Metadata readMicromanagerMetadata(final String n5RootUrl,
            final String datasetPath, final Context context)
    {
        final String json = readMetadataText(n5RootUrl, normalizeDataset(
                datasetPath));
        if (json == null || json.isEmpty()) return null;

        try {
            final MarsMicromanagerFormat.Parser parser =
                    new MarsMicromanagerFormat.Parser();
            final MarsMicromanagerFormat.Metadata source =
                    new MarsMicromanagerFormat.Metadata();

            if (source.getContext() == null) source.setContext(context);
            if (parser.getContext() == null) parser.setContext(context);

            // The parser expects one position per entry; the Mars N5 layout keeps
            // each position in its own dataset, so there is always exactly one.
            final List<MarsMicromanagerFormat.Position> positions =
                    new ArrayList<>();
            positions.add(new MarsMicromanagerFormat.Position());
            source.setPositions(positions);

            parser.populateMetadata(new String[] { json }, source, source, false);
            source.populateImageMetadata();
            return source;
        }
        catch (final IOException | FormatException | RuntimeException e) {
            // A malformed sidecar shouldn't block opening the pixels.
            System.out.println("Could not parse " + METADATA_FILE + " for "
                    + datasetUrl(n5RootUrl, datasetPath) + ": " + e.getMessage());
            return null;
        }
    }

    /**
     * Fetch the raw text of the {@code metadata.txt} beside a dataset, or null
     * if it does not exist. Handles both S3/MinIO roots and local directories.
     */
    public static String readMetadataText(final String n5RootUrl,
            final String datasetPath)
    {
        if (n5RootUrl == null || n5RootUrl.isEmpty()) return null;
        final String dataset = normalizeDataset(datasetPath);

        final MarsS3Browser.ParsedPath parsed = MarsS3Browser.parsePath(n5RootUrl);
        if (parsed == null) return readLocalMetadataText(n5RootUrl, dataset);

        try (final MarsS3Browser browser = new MarsS3Browser(parsed.server)) {
            final String key = joinKey(parsed.n5Root, dataset, METADATA_FILE);
            // Historically these keys were requested with a leading slash (see the
            // original Open N5 as ImagePlus command), so containers uploaded that
            // way store them under "/path/...". Try the canonical key first and
            // fall back, at the cost of one extra HEAD only when there is no
            // sidecar at all.
            for (final String candidate : Arrays.asList(key, "/" + key)) {
                final byte[] bytes = browser.getObjectBytes(parsed.bucket,
                        candidate);
                if (bytes != null) return new String(bytes,
                        StandardCharsets.UTF_8);
            }
            return null;
        }
        catch (final Exception e) {
            System.out.println("Could not read " + METADATA_FILE + " for "
                    + datasetUrl(n5RootUrl, dataset) + ": " + e.getMessage());
            return null;
        }
    }

    // ---- internals ----

    private static String readLocalMetadataText(final String n5RootUrl,
            final String dataset)
    {
        try {
            final File root = localRoot(n5RootUrl);
            // An empty dataset means the root itself, so the sidecar sits beside
            // the container's own attributes rather than inside a subgroup.
            final File dir = dataset.isEmpty() ? root : new File(root, dataset);
            final File file = new File(dir, METADATA_FILE);
            if (!file.isFile()) return null;
            return new String(Files.readAllBytes(file.toPath()),
                    StandardCharsets.UTF_8);
        }
        catch (final Exception e) {
            System.out.println("Could not read " + METADATA_FILE + " for "
                    + n5RootUrl + "/" + dataset + ": " + e.getMessage());
            return null;
        }
    }

    /** Local .n5 roots arrive either as a bare path or as a file: URI. */
    private static File localRoot(final String n5RootUrl) {
        try {
            final URI uri = new URI(n5RootUrl);
            if ("file".equalsIgnoreCase(uri.getScheme())) return new File(uri);
        }
        catch (final URISyntaxException | IllegalArgumentException e) {
            // Not a URI — treat it as a plain filesystem path.
        }
        return new File(n5RootUrl);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static <T extends NumericType<T> & NativeType<T>> Dataset
            buildDataset(final N5Reader n5, final String n5Path,
                    final String imageName, final Metadata metadata,
                    final boolean virtual, final Context context)
    {
        final CachedCellImg imgRaw = N5Utils.open(n5, n5Path);

        Img<T> img;
        if (virtual) {
            img = imgRaw;
        }
        else {
            final ExecutorService exec = Executors.newFixedThreadPool(8);
            try {
                final PlanarImg<T, ?> planarImg = new PlanarImgFactory<>(Util
                        .getTypeFromInterval((RandomAccessibleInterval<T>) imgRaw))
                                .create(imgRaw);
                LoopBuilder.setImages((RandomAccessibleInterval<T>) imgRaw,
                        planarImg).multiThreaded(new DefaultTaskExecutor(exec))
                        .forEachPixel((x, y) -> y.set(x));
                img = planarImg;
            }
            finally {
                exec.shutdown();
            }
        }

        final SCIFIOImgPlus<T> imgPlus = new SCIFIOImgPlus(img, imageName,
                axesFor(img.numDimensions()));
        if (metadata != null) {
            imgPlus.setMetadata(metadata);
            imgPlus.setImageMetadata(metadata.get(0));
        }

        return context.getService(DatasetService.class).create((ImgPlus) imgPlus);
    }

    private static AxisType[] axesFor(final int numDimensions) {
        final AxisType[] axes = new AxisType[numDimensions];
        for (int d = 0; d < numDimensions; d++)
            axes[d] = d < DEFAULT_AXIS_ORDER.length ? DEFAULT_AXIS_ORDER[d]
                    : Axes.unknown();
        return axes;
    }

    /**
     * Reduce a dataset path to its form relative to the .n5 root: no leading or
     * trailing slashes, but interior ones kept, since nested layouts such as
     * "dataset1/DNA" are a legitimate dataset address. "/" (the root itself)
     * normalizes to the empty string.
     */
    private static String normalizeDataset(final String datasetPath) {
        if (datasetPath == null) return "";
        String d = datasetPath;
        while (d.startsWith("/"))
            d = d.substring(1);
        while (d.endsWith("/"))
            d = d.substring(0, d.length() - 1);
        return d;
    }

    /** The .n5 container's own name, e.g. "acquisition.n5". */
    private static String rootName(final String n5RootUrl) {
        if (n5RootUrl == null || n5RootUrl.isEmpty()) return "n5";
        String s = n5RootUrl;
        while (s.endsWith("/"))
            s = s.substring(0, s.length() - 1);
        final int slash = s.lastIndexOf('/');
        final String name = slash >= 0 ? s.substring(slash + 1) : s;
        return name.isEmpty() ? "n5" : name;
    }

    /** The URL a dataset is stamped with, e.g. ".../foo.n5/Pos0". */
    private static String datasetUrl(final String n5RootUrl,
            final String datasetPath)
    {
        final String dataset = normalizeDataset(datasetPath);
        if (dataset.isEmpty()) return n5RootUrl;
        return n5RootUrl.endsWith("/") ? n5RootUrl + dataset
                : n5RootUrl + "/" + dataset;
    }

    private static String joinKey(final String... segments) {
        final StringBuilder sb = new StringBuilder();
        for (final String segment : segments) {
            if (segment == null || segment.isEmpty()) continue;
            String s = segment;
            while (s.startsWith("/"))
                s = s.substring(1);
            while (s.endsWith("/"))
                s = s.substring(0, s.length() - 1);
            if (s.isEmpty()) continue;
            if (sb.length() > 0) sb.append('/');
            sb.append(s);
        }
        return sb.toString();
    }
}
