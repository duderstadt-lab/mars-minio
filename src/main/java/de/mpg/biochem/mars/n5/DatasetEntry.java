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

import java.util.Arrays;
import java.util.stream.Collectors;

import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;

/**
 * Immutable description of a single N5 dataset (e.g. "Pos0") inside an .n5
 * container. Includes a preformatted info string matching the legacy
 * DatasetSelectorDialog presentation ("Pos0 (1024×1024×1×1×10, uint16) (21.0
 * MB)").
 *
 * @author Karl Duderstadt
 */
public class DatasetEntry {

    private final String name;
    private final long[] dimensions;
    private final DataType dataType;
    private final long numBytes;

    public DatasetEntry(final String name, final DatasetAttributes attributes) {
        this.name = name;
        this.dimensions = attributes.getDimensions();
        this.dataType = attributes.getDataType();

        long elements = 1L;
        for (long d : dimensions)
            elements *= d;
        // Uncompressed size: element count * bytes-per-element.
        this.numBytes = elements * (bitsPerElement(dataType) / 8L);
    }

    public String getName() {
        return name;
    }

    public long[] getDimensions() {
        return dimensions;
    }

    public DataType getDataType() {
        return dataType;
    }

    public long getNumBytes() {
        return numBytes;
    }

    /** Dimensions joined with the times sign, e.g. "1024×1024×1×1×10". */
    public String getDimensionString() {
        return Arrays.stream(dimensions).mapToObj(Long::toString).collect(
                Collectors.joining("×"));
    }

    /** Just the attribute summary: "Dimensions ..., uint16". Mirrors the
     * legacy BdvSourceOptionsPane.getDatasetInfo output (using " x "). */
    public String getAttributeInfo() {
        final String dimString = Arrays.stream(dimensions).mapToObj(Long::toString)
                .collect(Collectors.joining(" x "));
        return "Dimensions " + dimString + ", " + dataType;
    }

    /** Tree-row label: "Pos0 (1024×1024×1×1×10, uint16) (21.0 MB)". */
    public String getRowLabel() {
        return name + " (" + getDimensionString() + ", " + dataType + ") (" +
                humanReadableBytes(numBytes) + ")";
    }

    private static int bitsPerElement(final DataType type) {
        switch (type) {
            case INT8:
            case UINT8:
                return 8;
            case INT16:
            case UINT16:
                return 16;
            case INT32:
            case UINT32:
            case FLOAT32:
                return 32;
            case INT64:
            case UINT64:
            case FLOAT64:
                return 64;
            default:
                return 8;
        }
    }

    public static String humanReadableBytes(final long bytes) {
        if (bytes < 1024) return bytes + " B";
        final String[] units = { "KB", "MB", "GB", "TB", "PB" };
        double value = bytes / 1024.0;
        int unit = 0;
        while (value >= 1024.0 && unit < units.length - 1) {
            value /= 1024.0;
            unit++;
        }
        return String.format("%.1f %s", value, units[unit]);
    }
}
