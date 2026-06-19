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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.N5Reader;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;

/**
 * Focused S3/MinIO browser for the Mars use case: enumerate buckets, list
 * folder-level prefixes, list datasets inside an .n5 container, and assemble
 * the canonical Mars N5 URL.
 * <p>
 * Listing buckets and folders uses the AWS S3 client directly (folder-level
 * via the "/" delimiter). Listing datasets inside an .n5 uses the existing
 * {@link MarsN5ViewerReaderFun}, so it works for both S3 and local roots.
 *
 * @author Karl Duderstadt
 */
public class MarsS3Browser implements AutoCloseable {

    private final String serverUrl; // e.g. https://minio.sdmm.nat.tum.de:9000/
    private final AmazonS3 s3;

    public MarsS3Browser(final String serverUrl) {
        this.serverUrl = normalizeServer(serverUrl);
        this.s3 = buildClient(this.serverUrl);
    }

    /**
     * List all buckets visible to the current credentials. Throws (rather than
     * returning empty) if the server denies the global list operation, so the
     * caller can distinguish "no buckets" from "not permitted".
     */
    public List<String> listBuckets() {
        final List<String> names = new ArrayList<>();
        for (Bucket b : s3.listBuckets())
            names.add(b.getName());
        return names;
    }

    /**
     * List immediate child "folders" under the given prefix in a bucket, using
     * the "/" delimiter (returns S3 common prefixes). Prefix may be empty for
     * the bucket root. Returned names are the last path segment, without the
     * trailing slash.
     */
    public List<String> listFolders(final String bucket, final String prefix) {
        final String norm = (prefix == null || prefix.isEmpty()) ? "" : (prefix
                .endsWith("/") ? prefix : prefix + "/");

        final List<String> folders = new ArrayList<>();
        ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(
                bucket).withPrefix(norm).withDelimiter("/");

        ListObjectsV2Result result;
        do {
            result = s3.listObjectsV2(req);
            for (String cp : result.getCommonPrefixes()) {
                // cp looks like "23042025/" or "parent/child/"; take last segment.
                String trimmed = cp.endsWith("/") ? cp.substring(0, cp.length() - 1)
                        : cp;
                int slash = trimmed.lastIndexOf('/');
                folders.add(slash >= 0 ? trimmed.substring(slash + 1) : trimmed);
            }
            req.setContinuationToken(result.getNextContinuationToken());
        }
        while (result.isTruncated());

        return folders;
    }

    public boolean isN5(final String name) {
        return name != null && name.endsWith(".n5");
    }

    /**
     * List the datasets (top-level groups carrying array attributes) inside an
     * .n5 root. Works for any root URL the Mars reader understands (S3 or
     * local). Each entry carries dimensions, dtype and computed size.
     */
    public static List<DatasetEntry> listDatasets(final String n5RootUrl) {
        final List<DatasetEntry> entries = new ArrayList<>();
        final N5Reader reader = new MarsN5ViewerReaderFun().apply(n5RootUrl);
        if (reader == null) return entries;

        final String[] groups = reader.list("/");
        if (groups == null) return entries;

        for (String group : groups) {
            try {
                final DatasetAttributes attrs = reader.getDatasetAttributes(group);
                if (attrs != null) entries.add(new DatasetEntry(group, attrs));
            }
            catch (Exception e) {
                // Not a dataset (or unreadable) — skip it.
            }
        }
        return entries;
    }

    /**
     * Assemble the canonical Mars N5 URL from server + bucket + the path to the
     * .n5 root within the bucket. Produces e.g.
     * https://rnap2.s3.minio.sdmm.nat.tum.de:9000/23042025/...loading.n5
     */
    public static String buildPath(final String server, final String bucket,
                                   final String n5Root)
    {
        try {
            final URI uri = new URI(normalizeServer(server));
            final String scheme = uri.getScheme();
            final String host = uri.getHost();
            final int port = uri.getPort();

            final StringBuilder sb = new StringBuilder();
            sb.append(scheme).append("://");
            sb.append(bucket).append(".s3.").append(host);
            if (port > -1) sb.append(":").append(port);
            sb.append("/");

            String root = n5Root;
            while (root.startsWith("/"))
                root = root.substring(1);
            sb.append(root);
            return sb.toString();
        }
        catch (URISyntaxException e) {
            return server;
        }
    }

    @Override
    public void close() {
        try {
            s3.shutdown();
        }
        catch (Exception e) {
            // ignore
        }
    }

    // ---- internals ----

    private static String normalizeServer(final String server) {
        if (server == null) return "";
        String s = server.trim();
        // Keep scheme+host+port; drop any trailing path for the endpoint.
        return s;
    }

    /**
     * Build a path-style S3 client pointed at the given server endpoint. Mirrors
     * MarsN5Factory.createS3WithEndpoint; US_EAST_2 is a dummy region required by
     * the builder. Falls back to anonymous credentials if none are configured.
     */
    private static AmazonS3 buildClient(final String serverUrl) {
        AWSCredentials credentials = null;
        try {
            credentials = new DefaultAWSCredentialsProviderChain().getCredentials();
        }
        catch (final Exception e) {
            System.out.println(
                    "Could not load AWS credentials, falling back to anonymous.");
        }
        final AWSStaticCredentialsProvider credentialsProvider =
                new AWSStaticCredentialsProvider(credentials == null
                        ? new AnonymousAWSCredentials() : credentials);

        // Endpoint is scheme://host:port (strip any path).
        String endpoint = serverUrl;
        try {
            final URI uri = new URI(serverUrl);
            final StringBuilder ep = new StringBuilder();
            ep.append(uri.getScheme()).append("://").append(uri.getHost());
            if (uri.getPort() > -1) ep.append(":").append(uri.getPort());
            endpoint = ep.toString();
        }
        catch (URISyntaxException e) {
            // use as-is
        }

        return AmazonS3ClientBuilder.standard().withPathStyleAccessEnabled(true)
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                        endpoint, Regions.US_EAST_2.getName())).withCredentials(
                        credentialsProvider).build();
    }

    /** Parsed components of a canonical Mars N5 URL. */
    public static final class ParsedPath {
        public final String server;  // https://minio.sdmm.nat.tum.de:9000/
        public final String bucket;  // cmg
        public final String n5Root;  // 02092024/02092024-lane1/....n5 (no leading slash)

        public ParsedPath(String server, String bucket, String n5Root) {
            this.server = server;
            this.bucket = bucket;
            this.n5Root = n5Root;
        }
    }

    /**
     * Inverse of {@link #buildPath}. Parses a canonical Mars N5 URL of the form
     * scheme://bucket.s3.host[:port]/path into server, bucket and n5Root. Returns
     * null if the URL isn't in that form (e.g. a local path or unrecognized host).
     */
    public static ParsedPath parsePath(final String fullPath) {
        if (fullPath == null || fullPath.isEmpty()) return null;
        try {
            final URI uri = new URI(fullPath);
            final String scheme = uri.getScheme();
            final String host = uri.getHost();
            if (scheme == null || host == null) return null;

            // host = bucket.s3.<server-host>
            final int s3Idx = host.indexOf(".s3.");
            if (s3Idx < 0) return null;

            final String bucket = host.substring(0, s3Idx);
            final String serverHost = host.substring(s3Idx + ".s3.".length());

            final StringBuilder server = new StringBuilder();
            server.append(scheme).append("://").append(serverHost);
            if (uri.getPort() > -1) server.append(":").append(uri.getPort());
            server.append("/");

            String path = uri.getPath();
            while (path.startsWith("/"))
                path = path.substring(1);

            return new ParsedPath(server.toString(), bucket, path);
        }
        catch (URISyntaxException e) {
            return null;
        }
    }
}
