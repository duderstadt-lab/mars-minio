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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;

import org.janelia.saalfeldlab.googlecloud.GoogleCloudResourceManagerClient;
import org.janelia.saalfeldlab.googlecloud.GoogleCloudStorageURI;
import org.janelia.saalfeldlab.googlecloud.GoogleCloudUtils;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5Reader;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.googlecloud.N5GoogleCloudStorageReader;
import org.janelia.saalfeldlab.n5.googlecloud.N5GoogleCloudStorageWriter;
import org.janelia.saalfeldlab.n5.hdf5.N5HDF5Reader;
import org.janelia.saalfeldlab.n5.hdf5.N5HDF5Writer;
import org.janelia.saalfeldlab.n5.s3.N5AmazonS3Reader;
import org.janelia.saalfeldlab.n5.s3.N5AmazonS3Writer;
import org.janelia.saalfeldlab.n5.zarr.N5ZarrReader;
import org.janelia.saalfeldlab.n5.zarr.N5ZarrWriter;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.S3Utilities;
import com.google.cloud.resourcemanager.Project;
import com.google.cloud.resourcemanager.ResourceManager;
import com.google.cloud.storage.Storage;
import com.google.gson.GsonBuilder;

/**
 * Copy of N5Factory from Saalfeld lab, HHMI Janelia. BSD-2.
 *
 * Factory for various N5 readers and writers.  Implementation specific
 * parameters can be provided to the factory instance and will be used when
 * such implementations are generated and ignored otherwise. Reasonable
 * defaults are provided.
 *
 * This copy allows for a custom s3 endpoint to be added for AWS paths.
 * @author Karl Duderstadt
 *
 * @author Stephan Saalfeld
 * @author John Bogovic
 * @author Igor Pisarev
 */
public class MarsN5Factory implements Serializable {

    private static final long serialVersionUID = -6823715427289454617L;

    private static byte[] HDF5_SIG = {(byte)137, 72, 68, 70, 13, 10, 26, 10};
    private int[] hdf5DefaultBlockSize = {64, 64, 64, 1, 1};
    private boolean hdf5OverrideBlockSize = false;
    private GsonBuilder gsonBuilder = new GsonBuilder();
    private boolean cacheAttributes = true;
    private String zarrDimensionSeparator = ".";
    private boolean zarrMapN5DatasetAttributes = true;
    private boolean zarrMergeAttributes = true;
    private String googleCloudProjectId = null;

    public MarsN5Factory hdf5DefaultBlockSize(final int... blockSize) {

        hdf5DefaultBlockSize = blockSize;
        return this;
    }

    public MarsN5Factory hdf5OverrideBlockSize(final boolean override) {

        hdf5OverrideBlockSize = override;
        return this;
    }

    public MarsN5Factory gsonBuilder(final GsonBuilder gsonBuilder) {

        this.gsonBuilder = gsonBuilder;
        return this;
    }

    public MarsN5Factory zarrDimensionSeparator(final String separator) {

        zarrDimensionSeparator = separator;
        return this;
    }

    public MarsN5Factory zarrMapN5Attributes(final boolean mapAttributes) {

        zarrMapN5DatasetAttributes = mapAttributes;
        return this;
    }

    public MarsN5Factory googleCloudProjectId(final String projectId) {

        googleCloudProjectId = projectId;
        return this;
    }

    public static boolean isHDF5Writer(final String path) {

        if (path.contains(".h5") || path.contains(".hdf5"))
            return true;
        else
            return false;
    }

    public static boolean isHDF5Reader(final String path) throws IOException {

        if (Files.isRegularFile(Paths.get(path))) {
            /* optimistic */
            if (path.matches("(?i).*\\.(h5|hdf5)"))
                return true;
            else {
                try (final FileInputStream in = new FileInputStream(new File(path))) {
                    final byte[] sig = new byte[8];
                    in.read(sig);
                    return Arrays.equals(sig, HDF5_SIG);
                }
            }
        }
        return false;
    }

    /**
     * Helper method.
     *
     * @param url
     * @return
     */
    private static S3Client createS3(final String url) {

        final AwsCredentialsProvider credentialsProvider = resolveCredentialsProvider();
        final S3Uri uri = parseS3Uri(url);

        final S3ClientBuilder builder = S3Client.builder()
                .credentialsProvider(credentialsProvider);
        uri.region().ifPresent(builder::region);

        return builder.build();
    }

    /**
     * Helper method.
     *
     * @param endpoint
     * @return
     */
    private static S3Client createS3WithEndpoint(final String endpoint) {
        final AwsCredentialsProvider credentialsProvider = resolveCredentialsProvider();

        //US_EAST_2 is used as a dummy region.
        return S3Client.builder()
                .forcePathStyle(true)
                .endpointOverride(URI.create(endpoint))
                .region(Region.US_EAST_2)
                .credentialsProvider(credentialsProvider)
                .build();
    }

    private static AwsCredentialsProvider resolveCredentialsProvider() {
        try {
            final AwsCredentials credentials = DefaultCredentialsProvider.create()
                    .resolveCredentials();
            return StaticCredentialsProvider.create(credentials);
        } catch(final Exception e) {
            System.out.println( "Could not load AWS credentials, falling back to anonymous." );
            return AnonymousCredentialsProvider.create();
        }
    }

    /** Parses an "s3://bucket/key" (or virtual-hosted) URL without needing a live client. */
    private static S3Uri parseS3Uri(final String url) {
        return S3Utilities.builder().region(Region.US_EAST_1).build().parseUri(URI.create(url));
    }

    private static String bucketOf(final S3Uri s3uri, final String url) {
        return s3uri.bucket().orElseThrow(
                () -> new IllegalArgumentException("No bucket specified in " + url));
    }

    /**
     * Open an {@link N5Reader} for N5 filesystem.
     *
     * @param path path to the n5 root folder
     * @return the N5FsReader
     */
    public N5FSReader openFSReader(final String path) {

        return new N5FSReader(path, gsonBuilder);
    }

    /**
     * Open an {@link N5Reader} for Zarr.
     *
     * For more options of the Zarr backend study the {@link N5ZarrReader}
     * constructors.
     *
     * @param path path to the zarr directory
     * @return the N5ZarrReader
     */
    public N5ZarrReader openZarrReader(final String path) {

        return new N5ZarrReader(path, gsonBuilder, zarrMapN5DatasetAttributes, zarrMergeAttributes, cacheAttributes);
    }

    /**
     * Open an {@link N5Reader} for HDF5. Close the reader when you do not need
     * it any more.
     *
     * For more options of the HDF5 backend study the {@link N5HDF5Reader}
     * constructors.
     *
     * @param path path to the hdf5 file
     * @return the N5HDF5Reader
     */
    public N5HDF5Reader openHDF5Reader(final String path) {

        return new N5HDF5Reader(path, hdf5OverrideBlockSize, gsonBuilder, hdf5DefaultBlockSize);
    }

    /**
     * Open an {@link N5Reader} for Google Cloud.
     *
     * @param url url to the google cloud object
     * @return the N5GoogleCloudStorageReader
     */
    public N5GoogleCloudStorageReader openGoogleCloudReader(final String url) {

        final Storage storage = GoogleCloudUtils.createGoogleCloudStorage(null);
        final GoogleCloudStorageURI googleCloudUri = new GoogleCloudStorageURI(url);

        return new N5GoogleCloudStorageReader(
                storage,
                googleCloudUri.getBucket(),
                googleCloudUri.getKey(),
                gsonBuilder);
    }

    /**
     * Open an {@link N5Reader} for AWS S3.
     *
     * @param url url to the amazon s3 object
     * @return the N5AmazonS3Reader
     */
    public N5AmazonS3Reader openAWSS3Reader(final String url) {
        S3Uri s3uri = parseS3Uri(url);

        return new N5AmazonS3Reader(
                createS3(url),
                bucketOf(s3uri, url),
                s3uri.key().orElse(""),
                gsonBuilder);
    }

    /**
     * Open an {@link N5Reader} for AWS S3.
     *
     * @param s3Url url to the amazon s3 object
     * @param endpointUrl endpoint url for the server
     * @return the N5AmazonS3Reader
     */
    public N5AmazonS3Reader openAWSS3ReaderWithEndpoint(final String s3Url, final String endpointUrl) {
        final S3Client s3 = createS3WithEndpoint(endpointUrl);
        final S3Uri s3uri = s3.utilities().parseUri(URI.create(s3Url));

        return new N5AmazonS3Reader(
                s3,
                bucketOf(s3uri, s3Url),
                s3uri.key().orElse(""),
                gsonBuilder);
    }

    /**
     * Open an {@link N5Writer} for N5 filesystem.
     *
     * @param path path to the n5 directory
     * @return the N5FSWriter
     */
    public N5FSWriter openFSWriter(final String path) {
        return new N5FSWriter(path, gsonBuilder);
    }

    /**
     * Open an {@link N5Writer} for Zarr.
     *
     * For more options of the Zarr backend study the {@link N5ZarrWriter}
     * constructors.
     *
     * @param path path to the zarr directory
     * @return the N5ZarrWriter
     */
    public N5ZarrWriter openZarrWriter(final String path) {

        return new N5ZarrWriter(path, gsonBuilder, zarrDimensionSeparator, zarrMapN5DatasetAttributes, true);
    }

    /**
     * Open an {@link N5Writer} for HDF5.  Don't forget to close the writer
     * after writing to close the file and make it available to other
     * processes.
     *
     * For more options of the HDF5 backend study the {@link N5HDF5Writer}
     * constructors.
     *
     * @param path path to the hdf5 file
     * @return the N5HDF5Writer
     */
    public N5HDF5Writer openHDF5Writer(final String path) {
        return new N5HDF5Writer(path, hdf5OverrideBlockSize, gsonBuilder, hdf5DefaultBlockSize);
    }

    /**
     * Open an {@link N5Writer} for Google Cloud.
     *
     * @param url url to the google cloud object
     * @return the N5GoogleCloudStorageWriter
     */
    public N5GoogleCloudStorageWriter openGoogleCloudWriter(final String url) {
        String projectId = googleCloudProjectId;
        if (projectId == null) {
            final ResourceManager resourceManager = new GoogleCloudResourceManagerClient().create();
            final Iterator<Project> projectsIterator = resourceManager.list().iterateAll().iterator();
            if (!projectsIterator.hasNext())
                return null;
            projectId = projectsIterator.next().getProjectId();
        }

        final Storage storage = GoogleCloudUtils.createGoogleCloudStorage(projectId);
        final GoogleCloudStorageURI googleCloudUri = new GoogleCloudStorageURI(url);
        return new N5GoogleCloudStorageWriter(
                storage,
                googleCloudUri.getBucket(),
                googleCloudUri.getKey(),
                gsonBuilder);
    }

    /**
     * Open an {@link N5Writer} for AWS S3.
     *
     * @param url url to the s3 object
     * @return the N5AmazonS3Writer
     */
    public N5AmazonS3Writer openAWSS3Writer(final String url) {
        S3Uri s3uri = parseS3Uri(url);

        return new N5AmazonS3Writer(
                createS3(url),
                bucketOf(s3uri, url),
                s3uri.key().orElse(""),
                gsonBuilder);
    }

    /**
     * Open an {@link N5Writer} for AWS S3.
     *
     * @param s3Url url to the s3 object
     * @param endpointUrl endpoint url to the server
     * @return the N5AmazonS3Writer
     */
    public N5AmazonS3Writer openAWSS3WriterWithEndpoint(final String s3Url, final String endpointUrl) {
        final S3Client s3 = createS3WithEndpoint(endpointUrl);
        final S3Uri s3uri = s3.utilities().parseUri(URI.create(s3Url));

        return new N5AmazonS3Writer(
                s3,
                bucketOf(s3uri, s3Url),
                s3uri.key().orElse(""),
                gsonBuilder);
    }

    /**
     * Open an {@link N5Reader} based on some educated guessing from the url.
     *
     * @param url the location of the root location of the store
     * @return the N5Reader
     * @throws IOException the io exception
     */
    public N5Reader openReader(final String url) throws IOException {
        try {
            final URI uri = new URI(url);
            final String scheme = uri.getScheme();
            if (scheme == null);
            else if (scheme.equals("s3")) {
                return openAWSS3Reader(url);
            } else if (scheme.equals("gs"))
                return openGoogleCloudReader(url);
            else if (uri.getHost()!= null && scheme.equals("https") || scheme.equals("http")) {
                if (uri.getHost().matches(".*s3\\.amazonaws\\.com"))
                    return openAWSS3Reader(url);
                else if (uri.getHost().matches(".*cloud\\.google\\.com") || uri.getHost().matches(".*storage\\.googleapis\\.com"))
                    return openGoogleCloudReader(url);
                else if (uri.getHost().matches(".*s3\\..*")) {
                    String[] parts = uri.getHost().split("\\.",3);
                    String bucket = parts[0];
                    //Adding these slashes seems to overcome downstream processing of problematic paths like those with dates in the 12.12.2024 format
                    //in folder names. This could create issues in future release as they continue to change the path processing.
                    //This also ensures there is at least one slash when no path is provided when opened by N5AmazonS3Reader.
                    String path = "///" + uri.getPath();
                    String s3Url = "s3://" + bucket + path;
                    String endpointUrl = uri.getScheme() + "://" + parts[2] + ":" + uri.getPort();
                    return openAWSS3ReaderWithEndpoint(s3Url, endpointUrl);
                }
            }
        } catch (final URISyntaxException e) {}
        if (isHDF5Reader(url))
            return openHDF5Reader(url);
        else if (url.contains(".zarr"))
            return openZarrReader(url);
        else
            return openFSReader(url);
    }

    /**
     * Open an {@link N5Writer} based on some educated guessing from the url.
     *
     * @param url the location of the root location of the store
     * @return the N5Writer
     */
    public N5Writer openWriter(final String url) {

        try {
            final URI uri = new URI(url);
            final String scheme = uri.getScheme();
            if (scheme == null);
            else if (scheme.equals("s3"))
                return openAWSS3Writer(url);
            else if (scheme.equals("gs"))
                return openGoogleCloudWriter(url);
            else if (uri.getHost() != null && scheme.equals("https") || scheme.equals("http")) {
                if (uri.getHost().matches(".*s3\\.amazonaws\\.com"))
                    return openAWSS3Writer(url);
                else if (uri.getHost().matches(".*cloud\\.google\\.com") || uri.getHost().matches(".*storage\\.googleapis\\.com"))
                    return openGoogleCloudWriter(url);
                else if (uri.getHost().matches(".*s3\\..*")) {
                    String[] parts = uri.getHost().split("\\.",3);
                    String bucket = parts[0];
                    String s3Url = "s3://" + bucket + uri.getPath();
                    String endpointUrl = uri.getScheme() + "://" + parts[2] + ":" + uri.getPort();
                    return openAWSS3WriterWithEndpoint(s3Url, endpointUrl);
                }
            }
        } catch (final URISyntaxException e) {}
        if (isHDF5Writer(url))
            return openHDF5Writer(url);
        else if (url.matches("(?i).*\\.zarr"))
            return openZarrWriter(url);
        else
            return openFSWriter(url);
    }
}
