/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.gcp.gcs;

import com.google.auth.Credentials;
import com.google.cloud.gcs.analyticscore.client.GcsFileInfo;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemCache;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemImpl;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemOptions;
import com.google.cloud.gcs.analyticscore.client.GcsItemId;
import com.google.cloud.gcs.analyticscore.client.GcsItemInfo;
import com.google.cloud.gcs.analyticscore.client.GcsObjectRange;
import com.google.cloud.gcs.analyticscore.core.GcsAnalyticsCoreOptions;
import com.google.cloud.gcs.analyticscore.core.GoogleCloudStorageInputStream;
import com.google.cloud.storage.BlobId;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.FileIOMetricsContext;
import org.apache.iceberg.io.FileRange;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.MetricsContext;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.PropertyUtil;

/**
 * Gateway to the optional {@code com.google.cloud.gcs.analyticscore.*} dependency. All references
 * to analytics-core types are confined to this class so that it is loaded only when {@link
 * org.apache.iceberg.gcp.GCPProperties#GCS_ANALYTICS_CORE_ENABLED} is true.
 */
class AnalyticsCoreUtil {

  private AnalyticsCoreUtil() {}

  /**
   * Builds the credentials a file system reads with. Any resource whose lifetime must match the
   * file system, such as a token refresh handler holding an HTTP client, is registered into the
   * supplied {@link CloseableGroup} instead of the caller's own resources, so that it is closed with
   * the file system when its cache entry is evicted. That keeps token refresh working for a file
   * system shared through {@link GcsFileSystemCache} beyond the caller that created it.
   */
  @FunctionalInterface
  interface CredentialsFactory {
    /** Returns null to read with application-default credentials. */
    Credentials create(CloseableGroup credentialResources);
  }

  /**
   * Returns a file system shared across callers, keyed by {@code cacheKey}, building it with {@code
   * credentialsFactory} only on a cache miss. The key is a security boundary: callers reading with
   * the same key share cached object data, so it must identify the authorization held, not the
   * principal claimed, and must never be logged. The returned file system is owned by {@link
   * GcsFileSystemCache} and must not be closed by the caller.
   */
  static AutoCloseable getOrCreateFileSystem(
      String cacheKey, Map<String, String> properties, CredentialsFactory credentialsFactory) {
    Preconditions.checkState(
        PropertyUtil.propertyAsBoolean(properties, GCPProperties.GCS_ANALYTICS_CORE_ENABLED, false),
        "GCS analytics-core is disabled; %s must be set to true",
        GCPProperties.GCS_ANALYTICS_CORE_ENABLED);
    // Options parsing and credential building happen inside the factory so they run only on a cache
    // miss, not on every lookup of an already-shared file system.
    return GcsFileSystemCache.getOrCreate(
        cacheKey, () -> createFileSystem(properties, credentialsFactory));
  }

  private static GcsFileSystem createFileSystem(
      Map<String, String> properties, CredentialsFactory credentialsFactory) {
    GcsFileSystemOptions fileSystemOptions =
        new GcsAnalyticsCoreOptions("gcs.", properties).getGcsFileSystemOptions();
    CloseableGroup credentialResources = new CloseableGroup();
    Credentials credentials = credentialsFactory.create(credentialResources);
    try {
      // A null credential reads with application-default credentials and owns no resources.
      return credentials == null
          ? new GcsFileSystemImpl(fileSystemOptions)
          : new GcsFileSystemImpl(credentials, fileSystemOptions, credentialResources);
    } catch (RuntimeException | Error e) {
      // The file system never took ownership, so release the credential resources here.
      try {
        credentialResources.close();
      } catch (IOException closeError) {
        e.addSuppressed(closeError);
      }
      throw e;
    }
  }

  static SeekableInputStream newStream(
      AutoCloseable fileSystemHandle, BlobId blobId, Long blobSize, MetricsContext metrics)
      throws IOException {
    GcsFileSystem fileSystem = (GcsFileSystem) fileSystemHandle;
    GcsItemId itemId = gcsItemId(blobId);
    GoogleCloudStorageInputStream stream =
        blobSize == null
            ? GoogleCloudStorageInputStream.create(fileSystem, itemId)
            : GoogleCloudStorageInputStream.create(
                fileSystem, gcsFileInfo(blobId, itemId, blobSize));
    return new GcsInputStreamWrapper(stream, blobId, metrics);
  }

  private static GcsItemId gcsItemId(BlobId blobId) {
    GcsItemId.Builder builder =
        GcsItemId.builder().setBucketName(blobId.getBucket()).setObjectName(blobId.getName());
    if (blobId.getGeneration() != null) {
      builder.setContentGeneration(blobId.getGeneration());
    }

    return builder.build();
  }

  private static GcsFileInfo gcsFileInfo(BlobId blobId, GcsItemId itemId, long size) {
    GcsItemInfo itemInfo = GcsItemInfo.builder().setItemId(itemId).setSize(size).build();
    return GcsFileInfo.builder()
        .setItemInfo(itemInfo)
        .setUri(URI.create(blobId.toGsUtilUri()))
        .setAttributes(ImmutableMap.of())
        .build();
  }

  private static class GcsInputStreamWrapper extends SeekableInputStream implements RangeReadable {
    private final Counter readBytes;
    private final Counter readOperations;
    private final GoogleCloudStorageInputStream stream;
    private final BlobId blobId;

    GcsInputStreamWrapper(
        GoogleCloudStorageInputStream stream, BlobId blobId, MetricsContext metrics) {
      Preconditions.checkArgument(null != stream, "Invalid input stream : null");
      Preconditions.checkArgument(null != blobId, "Invalid blobId : null");
      this.stream = stream;
      this.blobId = blobId;
      this.readBytes = metrics.counter(FileIOMetricsContext.READ_BYTES, MetricsContext.Unit.BYTES);
      this.readOperations = metrics.counter(FileIOMetricsContext.READ_OPERATIONS);
    }

    @Override
    public long getPos() throws IOException {
      return stream.getPos();
    }

    @Override
    public void seek(long newPos) throws IOException {
      stream.seek(newPos);
    }

    @Override
    public int read() throws IOException {
      int readByte;
      try {
        readByte = stream.read();
      } catch (IOException e) {
        GCSExceptionUtil.throwNotFoundIfNotPresent(e, blobId);
        throw e;
      }
      readBytes.increment();
      readOperations.increment();
      return readByte;
    }

    @Override
    public int read(byte[] b) throws IOException {
      return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int bytesRead;
      try {
        bytesRead = stream.read(b, off, len);
      } catch (IOException e) {
        GCSExceptionUtil.throwNotFoundIfNotPresent(e, blobId);
        throw e;
      }
      if (bytesRead > 0) {
        readBytes.increment(bytesRead);
      }
      readOperations.increment();
      return bytesRead;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      try {
        stream.readFully(position, buffer, offset, length);
      } catch (IOException e) {
        GCSExceptionUtil.throwNotFoundIfNotPresent(e, blobId);
        throw e;
      }
    }

    @Override
    public int readTail(byte[] buffer, int offset, int length) throws IOException {
      try {
        return stream.readTail(buffer, offset, length);
      } catch (IOException e) {
        GCSExceptionUtil.throwNotFoundIfNotPresent(e, blobId);
        throw e;
      }
    }

    @Override
    public void readVectored(List<FileRange> ranges, IntFunction<ByteBuffer> allocate)
        throws IOException {
      List<GcsObjectRange> objectRanges =
          ranges.stream()
              .map(
                  fileRange ->
                      GcsObjectRange.builder()
                          .setOffset(fileRange.offset())
                          .setLength(fileRange.length())
                          .setByteBufferFuture(fileRange.byteBuffer())
                          .build())
              .collect(Collectors.toList());
      try {
        stream.readVectored(objectRanges, allocate);
      } catch (IOException e) {
        GCSExceptionUtil.throwNotFoundIfNotPresent(e, blobId);
        throw e;
      }
    }

    @Override
    public void close() throws IOException {
      stream.close();
    }
  }
}
