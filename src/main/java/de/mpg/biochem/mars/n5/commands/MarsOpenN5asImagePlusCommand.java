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

import com.amazonaws.services.s3.AmazonS3;
import ij.*;
import ij.io.FileInfo;
import net.imagej.DatasetService;
import org.janelia.saalfeldlab.n5.ij.N5Importer;
import org.janelia.saalfeldlab.n5.s3.N5AmazonS3Reader;
import org.scijava.ItemIO;
import org.scijava.app.StatusService;
import org.scijava.command.Command;
import org.scijava.command.DynamicCommand;
import org.scijava.convert.ConvertService;
import org.scijava.log.LogService;
import org.scijava.menu.MenuConstants;
import org.scijava.plugin.Menu;
import org.scijava.plugin.Parameter;
import org.scijava.plugin.Plugin;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import net.imagej.Dataset;
import ij.ImagePlus;
import net.imagej.DatasetService;
import net.imglib2.RandomAccessibleInterval;
import net.imglib2.img.ImagePlusAdapter;
import net.imglib2.img.Img;
import net.imglib2.img.display.imagej.ImageJFunctions;
import net.imglib2.interpolation.randomaccess.NLinearInterpolatorFactory;
import net.imglib2.realtransform.AffineTransform2D;
import net.imglib2.realtransform.RealViews;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.view.Views;

import de.mpg.biochem.mars.n5.*;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.ij.N5Importer.N5BasePathFun;
import org.janelia.saalfeldlab.n5.metadata.*;
import org.janelia.saalfeldlab.n5.metadata.canonical.CanonicalMetadataParser;
import org.janelia.saalfeldlab.n5.metadata.imagej.ImagePlusLegacyMetadataParser;
import org.janelia.saalfeldlab.n5.ui.DataSelection;
import org.janelia.saalfeldlab.n5.ui.DatasetSelectorDialog;
import org.janelia.saalfeldlab.n5.ui.N5DatasetTreeCellRenderer;
//import org.janelia.saalfeldlab.n5.universe.metadata.axes.AxisMetadata;
//import org.janelia.saalfeldlab.n5.universe.metadata.axes.AxisUtils;

@Plugin(type = Command.class, label = "Open N5 as ImagePlus", menu = { @Menu(
        label = MenuConstants.PLUGINS_LABEL, weight = MenuConstants.PLUGINS_WEIGHT,
        mnemonic = MenuConstants.PLUGINS_MNEMONIC), @Menu(label = "Mars",
        weight = MenuConstants.PLUGINS_WEIGHT, mnemonic = 'm'), @Menu(
        label = "Image", weight = 1, mnemonic = 'i'), @Menu(label = "Util",
        weight = 7, mnemonic = 'u'), @Menu(label = "Open N5 as ImagePlus",
        weight = 11, mnemonic = 'o') })
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
    private DatasetService datasetService;

    @Parameter(label = "image", type = ItemIO.OUTPUT)
    private Dataset dataset;

    @Override
    public void run() {
        DatasetSelectorDialog selectionDialog = new DatasetSelectorDialog(
                new MarsN5ViewerReaderFun(), new N5BasePathFun(),
                "", new N5MetadataParser[] {},
                new N5MetadataParser[] { new ImagePlusLegacyMetadataParser(),
                        new N5CosemMetadataParser(),
                        new N5SingleScaleMetadataParser(),
                        new CanonicalMetadataParser(),
                        new N5GenericSingleScaleMetadataParser() });

        selectionDialog.setVirtualOption(true);
        selectionDialog.setCropOption(false);

        selectionDialog.setTreeRenderer(new N5DatasetTreeCellRenderer(
                true));

        // Prevents NullPointerException
        selectionDialog.setContainerPathUpdateCallback(x -> {});

        final Consumer<DataSelection> callback = (
                DataSelection dataSelection) -> {
                    //Read metadata file
                    //MarsMicromanagerFormat.Parser

                    //parsePositionMV2(final String jsonData, final Metadata meta,
                    //final int posIndex)



                    String rootPath = selectionDialog.getN5RootPath();
                    String datasetPath = dataSelection.metadata.get(0).getPath();
                    N5DatasetMetadata datasetMeta = (N5DatasetMetadata) dataSelection.metadata.get(0);

                    //Build n5 reader
                    N5Reader n5 = new MarsN5ViewerReaderFun().apply(rootPath);
                    ExecutorService exec = Executors.newFixedThreadPool(8);

                    //System.out.println(((N5AmazonS3Reader) n5).getKeyValueAccess().lockForReading(datasetPath + "/metadata.txt").newInputStream());

                    System.out.println("datasetMeta " + dataSelection.metadata.get(0));

                    //if (datasetMeta != null && datasetMeta instanceof AxisMetadata) {
                    //    System.out.println("AxisMetadata " + (AxisMetadata) datasetMeta));
                    //}

                    ImagePlus imp;
                    try {
                        imp = N5Importer.read(n5, exec, datasetMeta, null, selectionDialog.isVirtual(), null);

                        FileInfo fileInfo = imp.getOriginalFileInfo();
                        if (fileInfo == null)
                            fileInfo = new FileInfo();

                        fileInfo.url = rootPath + "?" + datasetMeta.getPath();
                        imp.setFileInfo(fileInfo);
                        imp.show();
                    } catch (final IOException e) {
                        IJ.error("failed to read n5");
                    }
            };

        selectionDialog.run(callback);
    }

    private String getMetadataString(String url) {
        return "";
    }
}
