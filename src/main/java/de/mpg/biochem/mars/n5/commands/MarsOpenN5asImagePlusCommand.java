/*-
 * #%L
 * Molecule Archive Suite (Mars) - core data storage and processing algorithms.
 * %%
 * Copyright (C) 2018 - 2023 Karl Duderstadt
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
package de.mpg.biochem.mars.n5.commands;

import ij.*;
import ij.io.FileInfo;
import io.scif.FormatException;
import io.scif.img.SCIFIOImgPlus;
import net.imagej.DatasetService;
import net.imagej.ImgPlus;
import net.imagej.axis.AxisType;
import net.imglib2.img.planar.PlanarImg;
import net.imglib2.img.planar.PlanarImgFactory;
import org.apache.commons.io.IOUtils;
import org.janelia.saalfeldlab.n5.imglib2.N5Utils;
import org.janelia.saalfeldlab.n5.s3.N5AmazonS3Reader;
import org.janelia.saalfeldlab.n5.universe.metadata.N5DatasetMetadata;
import org.janelia.saalfeldlab.n5.universe.metadata.ome.ngff.v04.OmeNgffMetadataParser;
import org.scijava.app.StatusService;
import org.scijava.command.Command;
import org.scijava.command.DynamicCommand;
import org.scijava.convert.ConvertService;
import org.scijava.log.LogService;
import org.scijava.menu.MenuConstants;
import org.scijava.plugin.Menu;
import org.scijava.plugin.Parameter;
import org.scijava.plugin.Plugin;

import net.imagej.axis.Axes;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.concurrent.Executors;

import net.imglib2.cache.img.CachedCellImg;
import net.imglib2.type.numeric.NumericType;
import net.imagej.Dataset;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.Img;
import net.imglib2.type.NativeType;

import de.mpg.biochem.mars.n5.*;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.ij.N5Importer.N5BasePathFun;
import org.janelia.saalfeldlab.n5.metadata.imagej.ImagePlusLegacyMetadataParser;
import org.janelia.saalfeldlab.n5.ui.DataSelection;
import org.janelia.saalfeldlab.n5.ui.DatasetSelectorDialog;
import org.janelia.saalfeldlab.n5.ui.N5DatasetTreeCellRenderer;

import org.janelia.saalfeldlab.n5.universe.metadata.N5CosemMetadataParser;
import org.janelia.saalfeldlab.n5.universe.metadata.N5GenericSingleScaleMetadataParser;
import org.janelia.saalfeldlab.n5.universe.metadata.N5MetadataParser;
import org.janelia.saalfeldlab.n5.universe.metadata.N5SingleScaleMetadataParser;
import org.janelia.saalfeldlab.n5.universe.metadata.canonical.CanonicalMetadataParser;

import net.imglib2.loops.LoopBuilder;
import net.imglib2.util.Util;
import net.imglib2.parallel.DefaultTaskExecutor;
import org.scijava.ui.UIService;
import io.scif.Metadata;
import de.mpg.biochem.mars.scifio.MarsMicromanagerFormat;

@Plugin(type = Command.class, label = "Open N5 as ImagePlus", menu = { @Menu(
        label = MenuConstants.PLUGINS_LABEL, weight = MenuConstants.PLUGINS_WEIGHT,
        mnemonic = MenuConstants.PLUGINS_MNEMONIC), @Menu(label = "Mars",
        weight = MenuConstants.PLUGINS_WEIGHT, mnemonic = 'm'), @Menu(
        label = "Import", weight = 1, mnemonic = 'i'),  @Menu(label = "Open N5 as ImagePlus (minio)",
        weight = 20, mnemonic = 'o') })
public class MarsOpenN5asImagePlusCommand extends DynamicCommand implements Command {
    /**
     * SERVICES
     */
    @Parameter
    private LogService logService;

    @Parameter
    private StatusService statusService;

    @Parameter
    private ConvertService convertService;

    @Parameter
    private UIService uiService;

    @Parameter
    private DatasetService datasetService;

    public static final N5MetadataParser<?>[] PARSERS = new N5MetadataParser[]{
            new ImagePlusLegacyMetadataParser(),
            new N5CosemMetadataParser(),
            new N5SingleScaleMetadataParser(),
            new CanonicalMetadataParser(),
            new N5GenericSingleScaleMetadataParser()
    };

    @Override
    public void run() {
        DatasetSelectorDialog selectionDialog = new DatasetSelectorDialog(
                new MarsN5ViewerReaderFun(), new N5BasePathFun(),
                "",
                new N5MetadataParser[]{ new OmeNgffMetadataParser() }, // need the ngff parser because it's where the metadata are
                PARSERS);

        selectionDialog.setVirtualOption(true);
        selectionDialog.setCropOption(false);

        selectionDialog.setTreeRenderer(new N5DatasetTreeCellRenderer(
                true));

        // Prevents NullPointerException
        selectionDialog.setContainerPathUpdateCallback(x -> {});

        final Consumer<DataSelection> callback = (
                DataSelection dataSelection) -> {
                    String rootPath = selectionDialog.getN5RootPath();
                    String datasetPath = dataSelection.metadata.get(0).getPath();
                    N5DatasetMetadata datasetMeta = (N5DatasetMetadata) dataSelection.metadata.get(0);

                    //Build n5 reader
                    N5Reader n5 = new MarsN5ViewerReaderFun().apply(rootPath);

                    try {
                        InputStream inputStream = ((N5AmazonS3Reader) n5).getKeyValueAccess().lockForReading(datasetPath + "/metadata.txt").newInputStream();
                        String result = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
                        String[] jsonData = new String[1];
                        jsonData[0] = result;

                        MarsMicromanagerFormat.Parser parser = new MarsMicromanagerFormat.Parser();
                        MarsMicromanagerFormat.Metadata source = new MarsMicromanagerFormat.Metadata();

                        if (source.getContext() == null) source.setContext(getContext());
                        if (parser.getContext() == null) parser.setContext(getContext());

                        final List<MarsMicromanagerFormat.Position> positions = new ArrayList<>();
                        final MarsMicromanagerFormat.Position p = new MarsMicromanagerFormat.Position();
                        positions.add(p);
                        source.setPositions(positions);

                        parser.populateMetadata(jsonData, source, source, false);
                        source.populateImageMetadata();

                        Dataset dataset = getImage(n5, datasetMeta, source, selectionDialog.isVirtual());
                        dataset.setSource(rootPath + datasetPath);
                        uiService.show(dataset);
                    } catch (final IOException e) {
                        IJ.error("failed to read n5");
                    } catch (FormatException e) {
                        throw new RuntimeException(e);
                    }
        };

        selectionDialog.run(callback);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private <T extends NumericType<T> & NativeType<T>> Dataset getImage(final N5Reader n5, final N5DatasetMetadata datasetMeta, final Metadata metadata, final boolean asVirtual) {
        ExecutorService exec = Executors.newFixedThreadPool(8);

        final CachedCellImg imgRaw = N5Utils.open(n5, datasetMeta.getPath());

        AxisType[] axes = new AxisType[5];
        axes[0] = Axes.X;
        axes[1] = Axes.Y;
        axes[2] = Axes.Z;
        axes[3] = Axes.CHANNEL;
        axes[4] = Axes.TIME;

       Img<T> img;
        if (asVirtual) {
            img = imgRaw;
        } else {
            final PlanarImg<T, ?> planarImg = new PlanarImgFactory<>(Util.getTypeFromInterval((RandomAccessibleInterval<T>) imgRaw)).create(imgRaw);
            LoopBuilder.setImages((RandomAccessibleInterval<T>) imgRaw, planarImg)
                    .multiThreaded(new DefaultTaskExecutor(exec))
                    .forEachPixel((x, y) -> y.set(x));

            img = planarImg;
        }

        final SCIFIOImgPlus<T> imgPlus = new SCIFIOImgPlus(img, datasetMeta.getName(), axes);
        imgPlus.setMetadata(metadata);
        imgPlus.setImageMetadata(metadata.get(0));

        return datasetService.create((ImgPlus) imgPlus);
    }
}
