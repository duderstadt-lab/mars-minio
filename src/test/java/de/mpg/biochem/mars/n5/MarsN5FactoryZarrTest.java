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

import org.janelia.saalfeldlab.n5.DataType;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.FileSystemKeyValueAccess;
import org.janelia.saalfeldlab.n5.GzipCompression;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.zarr.N5ZarrWriter;
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3KeyValueReader;
import org.janelia.saalfeldlab.n5.zarr.v3.ZarrV3KeyValueWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.google.gson.GsonBuilder;

public class MarsN5FactoryZarrTest {

    @Test
    void containerNames() {
        assertTrue(MarsN5Factory.isContainerName("a.n5"));
        assertTrue(MarsN5Factory.isContainerName("a.ZARR/"));
        assertFalse(MarsN5Factory.isContainerName("a.n5.bak"));
        assertFalse(MarsN5Factory.isContainerName(null));
        assertTrue(MarsN5Factory.isZarr("https://b.s3.host:9000/x/y.zarr"));
        assertFalse(MarsN5Factory.isZarr("x.n5"));
    }

    @Test
    void localZarrV3(@TempDir final Path dir) throws Exception {
        final String root = dir.resolve("test.zarr").toString();
        try (final N5Writer w = new ZarrV3KeyValueWriter(new FileSystemKeyValueAccess(), root,
                new GsonBuilder(), true)) {
            w.createDataset("ds", new DatasetAttributes(new long[] {20, 20}, new int[] {10, 10},
                    DataType.UINT16, new GzipCompression()));
        }
        assertTrue(Files.exists(dir.resolve("test.zarr/zarr.json")));
        final N5Reader r = new MarsN5Factory().openReader(root);
        assertInstanceOf(ZarrV3KeyValueReader.class, r);
        assertArrayEquals(new long[] {20, 20}, r.getDatasetAttributes("ds").getDimensions());
    }

    @Test
    void localZarrV2(@TempDir final Path dir) throws Exception {
        final String root = dir.resolve("test.zarr").toString();
        try (final N5Writer w = new N5ZarrWriter(root)) {
            w.createDataset("ds", new DatasetAttributes(new long[] {20, 20}, new int[] {10, 10},
                    DataType.UINT16, new GzipCompression()));
        }
        final N5Reader r = new MarsN5Factory().openReader(root);
        assertFalse(r instanceof ZarrV3KeyValueReader);
        assertArrayEquals(new long[] {20, 20}, r.getDatasetAttributes("ds").getDimensions());
    }
}
