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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * A JVM-wide cache of analytics-core file systems, so that callers reading with the same
 * credentials and configuration share one GCS client, one read thread pool, and one set of object
 * caches instead of building a file system each.
 *
 * <p>Isolation is by key: a key identifies both the authorization a caller holds and the
 * configuration its file system was built with, so cached object data cannot reach a caller holding
 * different credentials. Keys are built by {@link AnalyticsCoreUtil}.
 *
 * <p>The cache owns the file systems it holds, along with the resources their credentials depend
 * on. Callers never close a file system; it is closed when its entry is evicted after going unused,
 * or when the cache is invalidated. A file system holds a GCS client and a pool of daemon threads,
 * so there is nothing that must be released before the JVM exits.
 *
 * <p>This class refers to analytics-core types and so, like {@link AnalyticsCoreUtil}, is only
 * loaded when {@link org.apache.iceberg.gcp.GCPProperties#GCS_ANALYTICS_CORE_ENABLED} is true.
 */
class GcsFileSystemCache {

  /**
   * How long an unused file system is kept. Every file opened through a file system counts as a
   * use, so this expires those belonging to credentials no longer in use, such as a vended token
   * that has been replaced.
   */
  private static final long EXPIRE_AFTER_ACCESS_MINUTES = 60;

  private static final Cache<String, CachedFileSystem> CACHE =
      Caffeine.newBuilder()
          .expireAfterAccess(EXPIRE_AFTER_ACCESS_MINUTES, TimeUnit.MINUTES)
          // Run maintenance on the calling thread so that a file system is closed before the
          // eviction or invalidation that removed it returns.
          .executor(Runnable::run)
          .removalListener(
              (String key, CachedFileSystem cached, RemovalCause cause) -> {
                if (null != cached) {
                  cached.close();
                }
              })
          .build();

  private GcsFileSystemCache() {}

  /**
   * Returns the file system for {@code key}, creating it with {@code factory} on first use. If
   * {@code factory} throws, nothing is cached and the exception is propagated.
   */
  static GcsFileSystem get(String key, Supplier<CachedFileSystem> factory) {
    Preconditions.checkArgument(null != key, "Invalid cache key: null");
    Preconditions.checkArgument(null != factory, "Invalid file system factory: null");

    CachedFileSystem cached = CACHE.get(key, ignored -> factory.get());
    Preconditions.checkState(null != cached, "Invalid file system: null");

    return cached.fileSystem();
  }

  /**
   * A file system and the resources it owns, which are closed together when the entry holding them
   * is removed. Credentials are built for a specific file system and can hold a refresh handler
   * with an HTTP client, which must live exactly as long as the file system using it.
   */
  static class CachedFileSystem {
    private final GcsFileSystem fileSystem;
    private final Closeable credentialResources;

    CachedFileSystem(GcsFileSystem fileSystem, Closeable credentialResources) {
      Preconditions.checkArgument(null != fileSystem, "Invalid file system: null");
      Preconditions.checkArgument(
          null != credentialResources, "Invalid credential resources: null");
      this.fileSystem = fileSystem;
      this.credentialResources = credentialResources;
    }

    GcsFileSystem fileSystem() {
      return fileSystem;
    }

    private void close() {
      try (Closeable resources = credentialResources) {
        fileSystem.close();
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to close credential resources", e);
      }
    }
  }

  /** Closes and removes every cached file system. */
  @VisibleForTesting
  static void invalidateAll() {
    CACHE.invalidateAll();
    CACHE.cleanUp();
  }

  @VisibleForTesting
  static long size() {
    CACHE.cleanUp();
    return CACHE.estimatedSize();
  }
}
