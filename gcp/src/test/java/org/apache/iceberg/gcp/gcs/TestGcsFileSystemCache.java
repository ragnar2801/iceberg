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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

public class TestGcsFileSystemCache {

  @AfterEach
  public void after() {
    GcsFileSystemCache.invalidateAll();
  }

  @Test
  public void sharedByKey() {
    GcsFileSystem fileSystem = mock(GcsFileSystem.class);
    AtomicInteger creations = new AtomicInteger();

    assertThat(GcsFileSystemCache.get("key", () -> cached(create(fileSystem, creations))))
        .isSameAs(fileSystem);
    assertThat(GcsFileSystemCache.get("key", () -> cached(create(fileSystem, creations))))
        .isSameAs(fileSystem);

    assertThat(creations.get()).isEqualTo(1);
    assertThat(GcsFileSystemCache.size()).isEqualTo(1);
  }

  @Test
  public void notSharedAcrossKeys() {
    GcsFileSystem fileSystem = mock(GcsFileSystem.class);
    GcsFileSystem otherFileSystem = mock(GcsFileSystem.class);

    assertThat(GcsFileSystemCache.get("key", () -> cached(fileSystem))).isSameAs(fileSystem);
    assertThat(GcsFileSystemCache.get("other-key", () -> cached(otherFileSystem)))
        .isSameAs(otherFileSystem);
    assertThat(GcsFileSystemCache.size()).isEqualTo(2);
  }

  @Test
  public void invalidateAllClosesFileSystems() {
    GcsFileSystem fileSystem = mock(GcsFileSystem.class);
    GcsFileSystem otherFileSystem = mock(GcsFileSystem.class);

    GcsFileSystemCache.get("key", () -> cached(fileSystem));
    GcsFileSystemCache.get("other-key", () -> cached(otherFileSystem));

    GcsFileSystemCache.invalidateAll();

    verify(fileSystem).close();
    verify(otherFileSystem).close();
    assertThat(GcsFileSystemCache.size()).isEqualTo(0);
  }

  @Test
  public void fileSystemIsRecreatedAfterInvalidation() {
    GcsFileSystem fileSystem = mock(GcsFileSystem.class);
    GcsFileSystem recreated = mock(GcsFileSystem.class);

    GcsFileSystemCache.get("key", () -> cached(fileSystem));
    GcsFileSystemCache.invalidateAll();

    assertThat(GcsFileSystemCache.get("key", () -> cached(recreated))).isSameAs(recreated);
  }

  @Test
  public void failedCreationCachesNothing() {
    assertThatThrownBy(
            () ->
                GcsFileSystemCache.get(
                    "key",
                    () -> {
                      throw new IllegalStateException("Failed to create credentials");
                    }))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Failed to create credentials");

    assertThat(GcsFileSystemCache.size()).isEqualTo(0);
  }

  @Test
  public void credentialResourcesAreClosedWithFileSystem() {
    GcsFileSystem fileSystem = mock(GcsFileSystem.class);
    AtomicBoolean credentialResourcesClosed = new AtomicBoolean();

    GcsFileSystemCache.get(
        "key",
        () ->
            new GcsFileSystemCache.CachedFileSystem(
                fileSystem, () -> credentialResourcesClosed.set(true)));

    GcsFileSystemCache.invalidateAll();

    verify(fileSystem).close();
    assertThat(credentialResourcesClosed).isTrue();
  }

  @Test
  public void invalidParameters() {
    GcsFileSystem fileSystem = mock(GcsFileSystem.class);

    assertThatThrownBy(() -> GcsFileSystemCache.get(null, () -> cached(fileSystem)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid cache key: null");

    assertThatThrownBy(() -> GcsFileSystemCache.get("key", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid file system factory: null");

    assertThatThrownBy(() -> GcsFileSystemCache.get("key", () -> null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Invalid file system: null");

    assertThatThrownBy(() -> new GcsFileSystemCache.CachedFileSystem(null, () -> {}))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid file system: null");

    assertThatThrownBy(() -> new GcsFileSystemCache.CachedFileSystem(fileSystem, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid credential resources: null");
  }

  private static GcsFileSystem create(GcsFileSystem fileSystem, AtomicInteger creations) {
    creations.incrementAndGet();
    return fileSystem;
  }

  private static GcsFileSystemCache.CachedFileSystem cached(GcsFileSystem fileSystem) {
    return new GcsFileSystemCache.CachedFileSystem(fileSystem, () -> {});
  }
}
