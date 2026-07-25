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

import com.google.cloud.gcs.analyticscore.client.GcsClientOptions;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystem;
import com.google.cloud.gcs.analyticscore.client.GcsFileSystemOptions;
import com.google.cloud.gcs.analyticscore.client.GcsReadOptions;
import java.util.Map;
import org.apache.iceberg.EnvironmentContext;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("resource")
public class TestPrefixedStorage {

  @AfterEach
  public void after() {
    GcsFileSystemCache.invalidateAll();
  }

  @Test
  public void invalidParameters() {
    assertThatThrownBy(() -> new PrefixedStorage(null, null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid storage prefix: null or empty");

    assertThatThrownBy(() -> new PrefixedStorage("", null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid storage prefix: null or empty");

    assertThatThrownBy(() -> new PrefixedStorage("gs://bucket", null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid properties: null");
  }

  @Test
  public void validParameters() {
    Map<String, String> properties =
        ImmutableMap.of(
            GCPProperties.GCS_PROJECT_ID, "myProject", GCPProperties.GCS_OAUTH2_TOKEN, "token");
    PrefixedStorage storage = new PrefixedStorage("gs://bucket", properties, null);

    assertThat(storage.storage()).isNotNull();
    assertThat(storage.storagePrefix()).isEqualTo("gs://bucket");
    assertThat(storage.gcpProperties().properties()).isEqualTo(properties);
  }

  @Test
  public void userAgentPrefix() {
    Map<String, String> properties =
        ImmutableMap.of(
            GCPProperties.GCS_PROJECT_ID, "myProject",
            GCPProperties.GCS_OAUTH2_TOKEN, "token",
            GCPProperties.GCS_USER_PROJECT, "myUserProject");
    PrefixedStorage storage = new PrefixedStorage("gs://bucket", properties, null);

    assertThat(storage.storage().getOptions().getUserAgent())
        .isEqualTo("gcsfileio/" + EnvironmentContext.get());
  }

  @Test
  public void impersonationPropertiesAreRead() {
    Map<String, String> properties =
        ImmutableMap.of(
            GCPProperties.GCS_PROJECT_ID, "myProject",
            GCPProperties.GCS_IMPERSONATE_SERVICE_ACCOUNT,
                "test-sa@project.iam.gserviceaccount.com",
            GCPProperties.GCS_IMPERSONATE_DELEGATES, "delegate-sa@project.iam.gserviceaccount.com",
            GCPProperties.GCS_IMPERSONATE_LIFETIME_SECONDS, "1800",
            GCPProperties.GCS_IMPERSONATE_SCOPES, "bigquery,devstorage.read_only");

    GCPProperties gcpProperties = new GCPProperties(properties);

    assertThat(gcpProperties.impersonateServiceAccount())
        .contains("test-sa@project.iam.gserviceaccount.com");
    assertThat(gcpProperties.impersonateDelegates())
        .contains("delegate-sa@project.iam.gserviceaccount.com");
    assertThat(gcpProperties.impersonateLifetimeSeconds()).isEqualTo(1800);
    assertThat(gcpProperties.impersonateScopes())
        .containsExactly(
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/devstorage.read_only");
  }

  @Test
  public void impersonationPropertiesWithDefaults() {
    Map<String, String> properties =
        ImmutableMap.of(
            GCPProperties.GCS_PROJECT_ID, "myProject",
            GCPProperties.GCS_IMPERSONATE_SERVICE_ACCOUNT,
                "test-sa@project.iam.gserviceaccount.com");

    GCPProperties gcpProperties = new GCPProperties(properties);

    assertThat(gcpProperties.impersonateServiceAccount())
        .contains("test-sa@project.iam.gserviceaccount.com");
    assertThat(gcpProperties.impersonateDelegates()).isNull();
    assertThat(gcpProperties.impersonateLifetimeSeconds())
        .isEqualTo(GCPProperties.GCS_IMPERSONATE_LIFETIME_SECONDS_DEFAULT);
  }

  @Test
  public void gcsFileSystemDisabledByDefault() {
    Map<String, String> properties = ImmutableMap.of(GCPProperties.GCS_PROJECT_ID, "myProject");
    PrefixedStorage storage = new PrefixedStorage("gs://bucket", properties, null);

    assertThat(storage.gcsFileSystem()).isNull();
  }

  @Test
  public void gcsFileSystem() {
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put(GCPProperties.GCS_ANALYTICS_CORE_ENABLED, "true")
            .put(GCPProperties.GCS_PROJECT_ID, "myProject")
            .put(GCPProperties.GCS_USER_PROJECT, "userProject")
            .put(GCPProperties.GCS_CLIENT_LIB_TOKEN, "gccl")
            .put(GCPProperties.GCS_SERVICE_HOST, "example.com")
            .put(GCPProperties.GCS_DECRYPTION_KEY, "decryptionKey")
            .put(GCPProperties.GCS_ENCRYPTION_KEY, "encryptionKey")
            .put(GCPProperties.GCS_CHANNEL_READ_CHUNK_SIZE, "1024")
            .build();
    PrefixedStorage storage = new PrefixedStorage("gs://bucket", properties, null);
    GcsFileSystemOptions expectedOptions =
        GcsFileSystemOptions.builder()
            .setGcsClientOptions(
                GcsClientOptions.builder()
                    .setProjectId("myProject")
                    .setClientLibToken("gccl")
                    .setServiceHost("example.com")
                    .setUserAgent("gcsfileio/" + EnvironmentContext.get())
                    .setGcsReadOptions(
                        GcsReadOptions.builder()
                            .setChunkSize(1024)
                            .setDecryptionKey("decryptionKey")
                            .setUserProjectId("userProject")
                            .build())
                    .build())
            .build();

    GcsFileSystem fileSystem = (GcsFileSystem) storage.gcsFileSystem();

    assertThat(fileSystem).isNotNull();
    assertThat(fileSystem.getGcsClient()).isNotNull();
    assertThat(fileSystem.getFileSystemOptions()).isEqualTo(expectedOptions);

    storage.close();
  }

  @Test
  public void credentialScopeForToken() {
    String secret = "ya29.vended-secret-value";
    String scope = credentialScope(ImmutableMap.of(GCPProperties.GCS_OAUTH2_TOKEN, secret));

    assertThat(scope)
        .isEqualTo(credentialScope(ImmutableMap.of(GCPProperties.GCS_OAUTH2_TOKEN, secret)));
    assertThat(scope)
        .isNotEqualTo(
            credentialScope(ImmutableMap.of(GCPProperties.GCS_OAUTH2_TOKEN, "ya29.other-secret")));
    assertThat(scope).contains("gs://bucket");
  }

  @Test
  public void credentialScopeForNoAuth() {
    String scope = credentialScope(ImmutableMap.of(GCPProperties.GCS_NO_AUTH, "true"));

    assertThat(scope)
        .isEqualTo(credentialScope(ImmutableMap.of(GCPProperties.GCS_NO_AUTH, "true")))
        .isNotEqualTo(credentialScope(ImmutableMap.of()))
        .isNotEqualTo(credentialScope(ImmutableMap.of(GCPProperties.GCS_OAUTH2_TOKEN, "token")));
  }

  @Test
  public void credentialScopeForImpersonation() {
    Map<String, String> properties =
        ImmutableMap.of(GCPProperties.GCS_IMPERSONATE_SERVICE_ACCOUNT, "sa@project.iam.test");
    String scope = credentialScope(properties);

    assertThat(scope).isEqualTo(credentialScope(properties));
    assertThat(scope)
        .isNotEqualTo(
            credentialScope(
                ImmutableMap.of(
                    GCPProperties.GCS_IMPERSONATE_SERVICE_ACCOUNT, "other-sa@project.iam.test")));

    // delegates and scopes change what the credential can do, so they change the identity
    assertThat(scope)
        .isNotEqualTo(
            credentialScope(
                ImmutableMap.of(
                    GCPProperties.GCS_IMPERSONATE_SERVICE_ACCOUNT, "sa@project.iam.test",
                    GCPProperties.GCS_IMPERSONATE_DELEGATES, "delegate@project.iam.test")));
    assertThat(scope)
        .isNotEqualTo(
            credentialScope(
                ImmutableMap.of(
                    GCPProperties.GCS_IMPERSONATE_SERVICE_ACCOUNT, "sa@project.iam.test",
                    GCPProperties.GCS_IMPERSONATE_SCOPES, "devstorage.read_only")));
  }

  @Test
  public void credentialScopeForApplicationDefault() {
    // application default credentials resolve to one identity per process
    assertThat(credentialScope(ImmutableMap.of()))
        .isEqualTo(credentialScope(ImmutableMap.of(GCPProperties.GCS_PROJECT_ID, "myProject")));
  }

  @Test
  public void credentialScopeDiffersByStoragePrefix() {
    Map<String, String> properties = ImmutableMap.of(GCPProperties.GCS_OAUTH2_TOKEN, "token");

    assertThat(PrefixedStorage.credentialScope(new GCPProperties(properties), "gs://bucket"))
        .isNotEqualTo(
            PrefixedStorage.credentialScope(new GCPProperties(properties), "gs://other-bucket"));
  }

  @Test
  public void credentialScopeFollowsCredentialPrecedence() {
    // no-auth takes precedence over impersonation when building credentials, so configurations that
    // resolve to the same credentials must resolve to the same scope
    Map<String, String> noAuth = ImmutableMap.of(GCPProperties.GCS_NO_AUTH, "true");
    Map<String, String> noAuthWithImpersonation =
        ImmutableMap.of(
            GCPProperties.GCS_NO_AUTH,
            "true",
            GCPProperties.GCS_IMPERSONATE_SERVICE_ACCOUNT,
            "sa@project.iam.test");

    assertThat(credentialScope(noAuth)).isEqualTo(credentialScope(noAuthWithImpersonation));
  }

  @Test
  public void gcsFileSystemIsSharedByCredentialScope() {
    Map<String, String> properties = analyticsCoreProperties("token");
    PrefixedStorage storage = new PrefixedStorage("gs://bucket", properties, null);
    PrefixedStorage sameCredentials = new PrefixedStorage("gs://bucket", properties, null);
    PrefixedStorage otherCredentials =
        new PrefixedStorage("gs://bucket", analyticsCoreProperties("other-token"), null);

    assertThat(storage.gcsFileSystem()).isSameAs(sameCredentials.gcsFileSystem());
    assertThat(storage.gcsFileSystem()).isNotSameAs(otherCredentials.gcsFileSystem());
  }

  @Test
  public void gcsFileSystemIsNotSharedAcrossFileSystemOptions() {
    Map<String, String> properties = analyticsCoreProperties("token");
    Map<String, String> otherOptions =
        ImmutableMap.<String, String>builder()
            .putAll(properties)
            .put(GCPProperties.GCS_CHANNEL_READ_CHUNK_SIZE, "1024")
            .build();
    PrefixedStorage storage = new PrefixedStorage("gs://bucket", properties, null);
    PrefixedStorage otherStorage = new PrefixedStorage("gs://bucket", otherOptions, null);

    assertThat(storage.gcsFileSystem()).isNotSameAs(otherStorage.gcsFileSystem());
  }

  @Test
  public void gcsFileSystemOutlivesTheStorageThatCreatedIt() {
    Map<String, String> properties = analyticsCoreProperties("token");
    PrefixedStorage storage = new PrefixedStorage("gs://bucket", properties, null);
    AutoCloseable fileSystem = storage.gcsFileSystem();

    // the cache owns the file system, so closing a storage must not close or drop it
    storage.close();

    PrefixedStorage reopened = new PrefixedStorage("gs://bucket", properties, null);

    assertThat(reopened.gcsFileSystem()).isSameAs(fileSystem);
  }

  private static Map<String, String> analyticsCoreProperties(String token) {
    return ImmutableMap.of(
        GCPProperties.GCS_ANALYTICS_CORE_ENABLED,
        "true",
        GCPProperties.GCS_SERVICE_HOST,
        "example.com",
        GCPProperties.GCS_OAUTH2_TOKEN,
        token);
  }

  private static String credentialScope(Map<String, String> properties) {
    return PrefixedStorage.credentialScope(new GCPProperties(properties), "gs://bucket");
  }
}
