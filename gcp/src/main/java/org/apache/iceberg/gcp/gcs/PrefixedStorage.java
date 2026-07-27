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

import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;
import java.util.TreeMap;
import org.apache.iceberg.EnvironmentContext;
import org.apache.iceberg.gcp.GCPAuthUtils;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.SerializableSupplier;

class PrefixedStorage implements AutoCloseable {
  private static final String GCS_FILE_IO_USER_AGENT = "gcsfileio/" + EnvironmentContext.get();
  private final String storagePrefix;
  private final GCPProperties gcpProperties;
  private final Map<String, String> propertiesWithUserAgent;
  private SerializableSupplier<Storage> storage;
  private CloseableGroup closeableGroup;
  private transient volatile Storage storageClient;
  private transient volatile String fileSystemCacheKey;

  PrefixedStorage(
      String storagePrefix, Map<String, String> properties, SerializableSupplier<Storage> storage) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(storagePrefix), "Invalid storage prefix: null or empty");
    Preconditions.checkArgument(null != properties, "Invalid properties: null");
    this.storagePrefix = storagePrefix;
    this.storage = storage;
    this.gcpProperties = new GCPProperties(properties);
    this.propertiesWithUserAgent =
        ImmutableMap.<String, String>builder()
            .putAll(properties)
            .put("gcs.user-agent", GCS_FILE_IO_USER_AGENT)
            .build();
    this.closeableGroup = new CloseableGroup();
    if (null == storage) {
      this.storage =
          () -> {
            StorageOptions.Builder builder =
                StorageOptions.newBuilder()
                    .setHeaderProvider(
                        FixedHeaderProvider.create(
                            ImmutableMap.of("User-agent", GCS_FILE_IO_USER_AGENT)));

            gcpProperties.projectId().ifPresent(builder::setProjectId);
            gcpProperties.clientLibToken().ifPresent(builder::setClientLibToken);
            gcpProperties.serviceHost().ifPresent(builder::setHost);

            // The storage client is owned by this instance, so its credential resources belong in
            // this instance's closeable group.
            Credentials credentials = credentials(gcpProperties, closeableGroup);
            if (credentials != null) {
              builder.setCredentials(credentials);
            }

            return builder.build().getService();
          };
    }
  }

  public String storagePrefix() {
    return storagePrefix;
  }

  public Storage storage() {
    if (null == storageClient) {
      synchronized (this) {
        if (null == storageClient) {
          this.storageClient = storage.get();
        }
      }
    }

    return storageClient;
  }

  public GCPProperties gcpProperties() {
    return gcpProperties;
  }

  @Override
  public void close() {
    try {
      // The analytics-core file system is owned by GcsFileSystemCache, not by this instance, so it
      // is intentionally not closed here.
      if (null != closeableGroup) {
        closeableGroup.close();
      }
    } catch (IOException ioe) {
      throw new UncheckedIOException(ioe);
    } finally {
      if (null != storage) {
        // GCS Storage does not appear to be closable, so release the reference
        storage = null;
      }
    }
  }

  // Returns AutoCloseable to avoid a runtime dependency on gcs-analytics-core. Cast via
  // AnalyticsCoreUtil.
  AutoCloseable gcsFileSystem() {
    if (!gcpProperties.isGcsAnalyticsCoreEnabled()) {
      return null;
    }

    // Look the file system up on every use rather than holding it in a field, so that an entry in
    // use keeps refreshing its access time in the cache and cannot expire underneath its user. The
    // credentials are built inside the factory, which runs only on a cache miss, so a shared entry
    // does not create a throwaway credential and refresh handler on every lookup.
    return AnalyticsCoreUtil.getOrCreateFileSystem(
        fileSystemCacheKey(),
        propertiesWithUserAgent,
        credentialResources -> credentials(gcpProperties, credentialResources));
  }

  /**
   * Derives the cache key that identifies the file system for this instance's credentials and
   * configuration. The key is hashed over the storage prefix and every property, so that any change
   * to either — a different credential above all — splits the cache rather than sharing a file
   * system with the wrong access. The digest keeps the credential-bearing material out of the key
   * as plaintext; the key must still never be logged.
   */
  private String fileSystemCacheKey() {
    if (fileSystemCacheKey == null) {
      synchronized (this) {
        if (fileSystemCacheKey == null) {
          StringBuilder material = new StringBuilder(storagePrefix).append('\n');
          // Sort so the key does not depend on property iteration order.
          new TreeMap<>(propertiesWithUserAgent)
              .forEach((key, value) -> material.append(key).append('=').append(value).append('\n'));
          try {
            byte[] digest =
                MessageDigest.getInstance("SHA-256")
                    .digest(material.toString().getBytes(StandardCharsets.UTF_8));
            this.fileSystemCacheKey = HexFormat.of().formatHex(digest);
          } catch (NoSuchAlgorithmException e) {
            // SHA-256 is required to be present on every Java platform.
            throw new IllegalStateException("SHA-256 is not available", e);
          }
        }
      }
    }

    return fileSystemCacheKey;
  }

  private Credentials credentials(GCPProperties properties, CloseableGroup credentialResources) {
    // Google Cloud APIs default to automatically detect the credentials to use, which is
    // in most cases the convenient way, especially in GCP.
    // See javadoc of com.google.auth.oauth2.GoogleCredentials.getApplicationDefault()
    if (properties.oauth2Token().isPresent()) {
      // The refresh handler, if any, is registered with credentialResources so its lifetime tracks
      // whoever owns these credentials: this instance's closeable group for the storage client, or
      // the shared file system for the analytics-core reader.
      return GCPAuthUtils.oauth2CredentialsFromGcpProperties(properties, credentialResources);
    } else if (properties.noAuth()) {
      // Explicitly allow "no credentials" for testing purposes
      return NoCredentials.getInstance();
    } else if (properties.impersonateServiceAccount().isPresent()) {
      return buildImpersonatedCredentials(properties);
    } else {
      return null;
    }
  }

  private Credentials buildImpersonatedCredentials(GCPProperties properties) {
    try {
      GoogleCredentials sourceCredentials = GoogleCredentials.getApplicationDefault();

      ImpersonatedCredentials impersonatedCredentials =
          ImpersonatedCredentials.create(
              sourceCredentials,
              properties.impersonateServiceAccount().get(),
              properties.impersonateDelegates(),
              properties.impersonateScopes(),
              properties.impersonateLifetimeSeconds());

      // Refresh to get initial token
      impersonatedCredentials.refresh();
      return impersonatedCredentials;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create impersonated credentials for GCS", e);
    }
  }
}
