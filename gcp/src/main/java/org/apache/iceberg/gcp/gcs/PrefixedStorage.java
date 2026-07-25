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
import java.util.Map;
import java.util.function.Supplier;
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

  // Null when analytics-core is disabled. Resolves the shared file system on each call; see
  // AnalyticsCoreUtil#fileSystemSupplier.
  private final Supplier<AutoCloseable> fileSystemSupplier;

  private SerializableSupplier<Storage> storage;
  private CloseableGroup closeableGroup;
  private transient volatile Storage storageClient;

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
    this.fileSystemSupplier =
        gcpProperties.isGcsAnalyticsCoreEnabled()
            ? AnalyticsCoreUtil.fileSystemSupplier(propertiesWithUserAgent, storagePrefix)
            : null;
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

            Credentials credentials = credentials(gcpProperties);
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

  /**
   * Returns the analytics-core file system for this storage, or null when analytics-core is
   * disabled. The file system is shared with other storages holding the same credentials and
   * configuration and is owned by {@link GcsFileSystemCache}, so it outlives this storage and must
   * not be closed here.
   *
   * <p>Returns AutoCloseable to avoid a runtime dependency on gcs-analytics-core. Cast via
   * AnalyticsCoreUtil.
   */
  AutoCloseable gcsFileSystem() {
    return null != fileSystemSupplier ? fileSystemSupplier.get() : null;
  }

  /** The credential mechanism a configuration selects. */
  private enum AuthType {
    TOKEN,
    NO_AUTH,
    IMPERSONATION,
    APPLICATION_DEFAULT
  }

  /**
   * Returns the credential mechanism {@code properties} selects. This is the single place the
   * precedence between mechanisms is defined, so that credentials and the credential scope derived
   * from them cannot disagree about which one applies.
   */
  private static AuthType authType(GCPProperties properties) {
    // Google Cloud APIs default to automatically detect the credentials to use, which is
    // in most cases the convenient way, especially in GCP.
    // See javadoc of com.google.auth.oauth2.GoogleCredentials.getApplicationDefault()
    if (properties.oauth2Token().isPresent()) {
      return AuthType.TOKEN;
    } else if (properties.noAuth()) {
      // Explicitly allow "no credentials" for testing purposes
      return AuthType.NO_AUTH;
    } else if (properties.impersonateServiceAccount().isPresent()) {
      return AuthType.IMPERSONATION;
    } else {
      return AuthType.APPLICATION_DEFAULT;
    }
  }

  private Credentials credentials(GCPProperties properties) {
    return credentialsFrom(properties, closeableGroup);
  }

  /**
   * Builds credentials for {@code properties}, registering anything that needs closing (such as a
   * vended credentials refresh handler) with {@code closeables}.
   */
  static Credentials credentialsFrom(GCPProperties properties, CloseableGroup closeables) {
    switch (authType(properties)) {
      case TOKEN:
        return GCPAuthUtils.oauth2CredentialsFromGcpProperties(properties, closeables);
      case NO_AUTH:
        return NoCredentials.getInstance();
      case IMPERSONATION:
        return buildImpersonatedCredentials(properties);
      case APPLICATION_DEFAULT:
        return null;
      default:
        throw new IllegalStateException("Unknown auth type: " + authType(properties));
    }
  }

  /**
   * Returns an identifier for the authorization {@code properties} carries, used to keep file
   * systems with different access from sharing cached object data.
   *
   * <p>Values that identify equal access must produce equal scopes and nothing else may: an
   * over-specific scope only costs cache hits, while an under-specific one lets a caller read data
   * fetched with credentials it does not hold.
   *
   * <p>A scope can carry a credential, as {@link org.apache.iceberg.rest.auth.AuthSessionCache}
   * keys do, so it must not be logged.
   */
  static String credentialScope(GCPProperties properties, String storagePrefix) {
    switch (authType(properties)) {
      case TOKEN:
        // The token is the grant itself, so equal tokens mean equal access however the token was
        // obtained.
        return storagePrefix + "|token:" + properties.oauth2Token().get();
      case NO_AUTH:
        // Unauthenticated callers all have the same (empty) authorization. The endpoint they reach
        // is part of the file system options, which are the other half of the cache key.
        return storagePrefix + "|no-auth";
      case IMPERSONATION:
        // Delegates and scopes change what the minted credential can do and who is allowed to mint
        // it, so both are part of the identity, not just the target service account.
        return storagePrefix
            + "|service-account:"
            + properties.impersonateServiceAccount().get()
            + "|delegates:"
            + properties.impersonateDelegates()
            + "|scopes:"
            + properties.impersonateScopes();
      case APPLICATION_DEFAULT:
        // Application default credentials resolve to a single identity per process.
        return storagePrefix + "|application-default";
      default:
        throw new IllegalStateException("Unknown auth type: " + authType(properties));
    }
  }

  private static Credentials buildImpersonatedCredentials(GCPProperties properties) {
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
