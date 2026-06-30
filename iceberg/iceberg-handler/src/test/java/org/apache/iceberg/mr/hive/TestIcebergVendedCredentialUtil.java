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

package org.apache.iceberg.mr.hive;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsStorageCredentials;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link IcebergVendedCredentialUtil}: copying REST-vended storage credentials into Hive
 * job configuration at plan time and re-applying them on executors.
 */
public class TestIcebergVendedCredentialUtil {

  /**
   * Tests {@link IcebergVendedCredentialUtil#requestsVendedCredentials(Properties, Configuration)}.
   *
   * <p><b>Setup:</b> table properties name catalog {@code ice01}; session {@code conf} has no
   * delegation header, then gains {@code vended-credentials}.
   *
   * <p><b>Expects:</b> {@code false} without the header, {@code true} with it — propagation and
   * apply logic must not run for ordinary REST catalogs.
   *
   * <p><b>Why:</b> Iceberg REST only vends storage credentials when the client sends
   * {@code X-Iceberg-Access-Delegation: vended-credentials}; Hive mirrors that opt-in via catalog
   * config so static catalog keys are not replaced unnecessarily.
   */
  @Test
  public void requestsVendedCredentialsRequiresDelegationHeader() {
    Configuration conf = new Configuration();
    Properties props = new java.util.Properties();
    props.setProperty(InputFormatConfig.CATALOG_NAME, "ice01");

    assertThat(IcebergVendedCredentialUtil.requestsVendedCredentials(props, conf)).isFalse();

    conf.set(
        "iceberg.catalog.ice01.header.X-Iceberg-Access-Delegation",
        "vended-credentials");
    assertThat(IcebergVendedCredentialUtil.requestsVendedCredentials(props, conf)).isTrue();
  }

  /**
   * Tests {@link IcebergVendedCredentialUtil#propagateToJob} when both {@code jobProperties} and
   * {@code jobSecrets} are non-null (full compile-time path after both storage-handler hooks).
   *
   * <p><b>Setup:</b> vended credential with internal endpoint {@code http://minio:9000}; session
   * {@code conf} sets {@code iceberg.catalog.ice01.s3.endpoint} to {@code http://host:9000}.
   *
   * <p><b>Expects:</b> endpoint and path-style in {@code jobProps} (Iceberg catalog keys and S3A
   * per-bucket keys) with host endpoint; access/secret and serialized blob in {@code jobSecrets}
   * only; blob endpoint also host; no secret material in {@code jobProps}.
   *
   * <p><b>Why:</b> HIVE-20651 keeps secrets in {@code TableDesc#jobSecrets} (later Hadoop
   * {@code Credentials}) and non-secrets in job conf. Catalogs often vend an internal S3 endpoint;
   * HS2 overrides with a reachable host endpoint at propagation time so Tez tasks and Iceberg FileIO
   * agree on connectivity while still using vended keys.
   */
  @Test
  public void propagateToJobMapsIcebergAndS3aProperties() {
    Configuration conf = new Configuration();
    conf.set("iceberg.catalog.ice01." + IcebergVendedCredentialUtil.ENDPOINT, "http://host:9000");

    CredentialFileIO fileIO = new CredentialFileIO();
    fileIO.setCredentials(
        List.of(
            StorageCredential.create(
                "s3://my-bucket/",
                Map.of(
                    IcebergVendedCredentialUtil.ACCESS_KEY_ID, "access",
                    IcebergVendedCredentialUtil.SECRET_ACCESS_KEY, "secret",
                    IcebergVendedCredentialUtil.ENDPOINT, "http://minio:9000",
                    IcebergVendedCredentialUtil.PATH_STYLE_ACCESS, "true"))));

    Table table =
        new BaseTable(new StaticTableOperations("s3://my-bucket/t", fileIO), "db.t");
    Map<String, String> jobProps = Maps.newHashMap();
    Map<String, String> jobSecrets = Maps.newHashMap();

    IcebergVendedCredentialUtil.propagateToJob(table, "ice01", jobProps, jobSecrets, conf);

    assertThat(jobProps)
        .containsEntry(
            "iceberg.catalog.ice01." + IcebergVendedCredentialUtil.ENDPOINT,
            "http://host:9000")
        .containsEntry(
            "fs.s3a.bucket.my-bucket.endpoint",
            "http://host:9000")
        .containsEntry(
            "fs.s3a.bucket.my-bucket.path.style.access",
            "true")
        .doesNotContainKey(InputFormatConfig.VENDED_STORAGE_CREDENTIALS)
        .doesNotContainKey("fs.s3a.bucket.my-bucket.access.key")
        .doesNotContainKey(
            "iceberg.catalog.ice01." + IcebergVendedCredentialUtil.ACCESS_KEY_ID);

    assertThat(jobSecrets)
        .containsEntry("fs.s3a.bucket.my-bucket.access.key", "access")
        .containsEntry("fs.s3a.bucket.my-bucket.secret.key", "secret")
        .doesNotContainKey(
            "iceberg.catalog.ice01." + IcebergVendedCredentialUtil.ACCESS_KEY_ID)
        .doesNotContainKey(
            "iceberg.catalog.ice01." + IcebergVendedCredentialUtil.SECRET_ACCESS_KEY)
        .satisfies(map ->
            assertThat(map.get(InputFormatConfig.VENDED_STORAGE_CREDENTIALS))
                .isNotBlank());

    List<StorageCredential> serialized =
        SerializationUtil.deserializeFromBase64(
            jobSecrets.get(InputFormatConfig.VENDED_STORAGE_CREDENTIALS));

    assertThat(serialized.getFirst().prefix()).isEqualTo("s3://my-bucket/");
    assertThat(serialized.getFirst().config())
        .containsEntry(IcebergVendedCredentialUtil.ENDPOINT, "http://host:9000")
        .containsEntry(IcebergVendedCredentialUtil.ACCESS_KEY_ID, "access")
        .containsEntry(IcebergVendedCredentialUtil.SECRET_ACCESS_KEY, "secret")
        .containsEntry(IcebergVendedCredentialUtil.PATH_STYLE_ACCESS, "true");
  }

  /**
   * Tests {@link IcebergVendedCredentialUtil#propagateToJob} with {@code jobSecrets == null}.
   *
   * <p><b>Setup:</b> same mixed secret/non-secret credential as the full test; only
   * {@code jobProperties} map is passed (models {@code configureInputJobProperties} /
   * {@code overlayTableProperties}).
   *
   * <p><b>Expects:</b> overridden endpoint in {@code jobProps}; no serialized blob and no Iceberg
   * access-key property in {@code jobProps}.
   *
   * <p><b>Why:</b> Hive calls the job-properties hook without a secrets map. This hook must add
   * connectivity settings for S3A and Iceberg but must not place keys or the credential blob in
   * plain job properties — those are filled later by {@code configureInputJobCredentials}.
   */
  @Test
  public void propagateNonSecretsOnlyWhenJobSecretsNull() {
    Configuration conf = new Configuration();
    conf.set("iceberg.catalog.ice01." + IcebergVendedCredentialUtil.ENDPOINT, "http://host:9000");

    CredentialFileIO fileIO = new CredentialFileIO();
    fileIO.setCredentials(
        List.of(
            StorageCredential.create(
                "s3://my-bucket/",
                Map.of(
                    IcebergVendedCredentialUtil.ACCESS_KEY_ID, "access",
                    IcebergVendedCredentialUtil.SECRET_ACCESS_KEY, "secret",
                    IcebergVendedCredentialUtil.ENDPOINT, "http://minio:9000",
                    IcebergVendedCredentialUtil.PATH_STYLE_ACCESS, "true"))));

    Table table =
        new BaseTable(new StaticTableOperations("s3://my-bucket/t", fileIO), "db.t");
    Map<String, String> jobProps = Maps.newHashMap();

    IcebergVendedCredentialUtil.propagateToJob(table, "ice01", jobProps, null, conf);

    assertThat(jobProps)
        .containsEntry(
            "iceberg.catalog.ice01." + IcebergVendedCredentialUtil.ENDPOINT,
            "http://host:9000")
        .containsEntry("fs.s3a.bucket.my-bucket.endpoint", "http://host:9000")
        .containsEntry("fs.s3a.bucket.my-bucket.path.style.access", "true")
        .doesNotContainKey(InputFormatConfig.VENDED_STORAGE_CREDENTIALS)
        .doesNotContainKey(
            "iceberg.catalog.ice01." + IcebergVendedCredentialUtil.ACCESS_KEY_ID);
  }

  /**
   * Tests {@link IcebergVendedCredentialUtil#extractCredentials(Table)} when
   * {@link SupportsStorageCredentials#credentials()} is empty.
   *
   * <p><b>Setup:</b> FileIO initialized with {@code s3.access-key-id} / {@code s3.secret-access-key}
   * in {@link FileIO#properties()} but an empty credential list.
   *
   * <p><b>Expects:</b> one synthetic {@link StorageCredential} with the access key from properties.
   *
   * <p><b>Why:</b> after {@code loadTable}, some REST clients populate {@code FileIO} properties
   * before the credential list; propagation must still find vended keys or plan/launch would skip
   * credential vending even though the table load succeeded.
   */
  @Test
  public void extractCredentialsFromFileIoPropertiesWhenCredentialListEmpty() {
    CredentialFileIO fileIO = new CredentialFileIO();
    fileIO.initialize(
        Map.of(
            IcebergVendedCredentialUtil.ACCESS_KEY_ID, "access",
            IcebergVendedCredentialUtil.SECRET_ACCESS_KEY, "secret",
            IcebergVendedCredentialUtil.ENDPOINT, "http://minio:9000"));
    Schema schema = new Schema(Types.NestedField.required(1, "x", Types.IntegerType.get()));
    TableMetadata metadata =
        TableMetadata.newTableMetadata(schema, PartitionSpec.unpartitioned(), "s3://my-bucket/warehouse/t", Map.of());
    Table table = new BaseTable(new StaticTableOperations(metadata, fileIO), "db.t");

    assertThat(IcebergVendedCredentialUtil.extractCredentials(table)).hasSize(1);
    StorageCredential extracted = IcebergVendedCredentialUtil.extractCredentials(table).getFirst();
    assertThat(extracted.prefix()).isEqualTo("s3://my-bucket/");
    assertThat(extracted.config())
        .containsEntry(IcebergVendedCredentialUtil.ACCESS_KEY_ID, "access")
        .containsEntry(IcebergVendedCredentialUtil.SECRET_ACCESS_KEY, "secret")
        .containsEntry(IcebergVendedCredentialUtil.ENDPOINT, "http://minio:9000");
  }

  /**
   * Tests {@link IcebergVendedCredentialUtil#propagateToJob} with {@code jobProperties == null}.
   *
   * <p><b>Setup:</b> credential with access key and secret only; only {@code jobSecrets} map is
   * passed (models {@code configureInputJobCredentials} for input and output tables).
   *
   * <p><b>Expects:</b> serialized blob and {@code fs.s3a.bucket.*.access.key/secret.key} in
   * {@code jobSecrets}; no catalog-prefixed secret keys, no endpoint in {@code jobSecrets}.
   *
   * <p><b>Why:</b> the credentials hook receives only the secrets map. Propagation must still run
   * without NPE, write material for {@code PlanUtils#configureJobConf} to move into
   * {@code Credentials}, and avoid duplicating non-secrets or catalog-level secret keys that belong
   * in job properties or only in the serialized blob.
   */
  @Test
  public void propagateSecretsOnlyWhenJobPropertiesNull() {
    CredentialFileIO fileIO = new CredentialFileIO();
    fileIO.setCredentials(
        List.of(
            StorageCredential.create(
                "s3://my-bucket/",
                Map.of(IcebergVendedCredentialUtil.ACCESS_KEY_ID, "access",
                    IcebergVendedCredentialUtil.SECRET_ACCESS_KEY, "secret"))));
    Table table =
        new BaseTable(new StaticTableOperations("s3://my-bucket/t", fileIO), "db.t");
    Map<String, String> jobSecrets = Maps.newHashMap();

    IcebergVendedCredentialUtil.propagateToJob(table, "ice01", null, jobSecrets, new Configuration());

    assertThat(jobSecrets)
        .containsEntry("fs.s3a.bucket.my-bucket.access.key", "access")
        .containsEntry("fs.s3a.bucket.my-bucket.secret.key", "secret")
        .doesNotContainKey(
            "iceberg.catalog.ice01." + IcebergVendedCredentialUtil.ACCESS_KEY_ID)
        .doesNotContainKey(
            "iceberg.catalog.ice01." + IcebergVendedCredentialUtil.SECRET_ACCESS_KEY)
        .doesNotContainKey("fs.s3a.bucket.my-bucket.endpoint")
        .satisfies(map ->
            assertThat(map.get(InputFormatConfig.VENDED_STORAGE_CREDENTIALS))
                .isNotBlank());

    List<StorageCredential> serialized =
        SerializationUtil.deserializeFromBase64(
            jobSecrets.get(InputFormatConfig.VENDED_STORAGE_CREDENTIALS));

    assertThat(serialized.getFirst().prefix()).isEqualTo("s3://my-bucket/");
    assertThat(serialized.getFirst().config())
        .containsEntry(IcebergVendedCredentialUtil.ACCESS_KEY_ID, "access")
        .containsEntry(IcebergVendedCredentialUtil.SECRET_ACCESS_KEY, "secret");
  }

  /**
   * Tests {@link IcebergVendedCredentialUtil#applyFromJobConf} for a non-vending catalog.
   *
   * <p><b>Setup:</b> task {@code conf} has catalog name only — no serialized credentials blob and
   * no {@code vended-credentials} delegation header.
   *
   * <p><b>Expects:</b> FileIO credential list stays empty after apply.
   *
   * <p><b>Why:</b> {@link IcebergVendedCredentialUtil#shouldSkipApplyFromJobConf} must return early
   * so tables that use static catalog S3 configuration are not cleared or rewritten on executors.
   */
  @Test
  public void applyFromJobConfSkipsWhenVendedCredentialsNotRequested() {
    CredentialFileIO fileIO = new CredentialFileIO();
    Table table =
        new BaseTable(new StaticTableOperations("s3://my-bucket/t", fileIO), "db.t");

    Configuration conf = new Configuration();
    conf.set("iceberg.catalog", "ice01");

    IcebergVendedCredentialUtil.applyFromJobConf(table, conf);

    assertThat(fileIO.credentials()).isEmpty();
  }

  /**
   * Tests {@link IcebergVendedCredentialUtil#applyFromJobConf} endpoint override on an existing FileIO.
   *
   * <p><b>Setup:</b> FileIO already has vended credentials with {@code http://minio:9000}; task
   * {@code conf} requests vending and sets host endpoint {@code http://host:9000}.
   *
   * <p><b>Expects:</b> after apply, FileIO credential endpoint is {@code http://host:9000}.
   *
   * <p><b>Why:</b> HS2 may retain credentials from {@code loadTable} with the catalog-internal
   * endpoint; apply must re-merge session catalog S3 settings so commit paths and tasks use the
   * same reachable endpoint as plan-time propagation.
   */
  @Test
  public void applyFromJobConfOverridesEndpointOnExistingCredentials() {
    CredentialFileIO fileIO = new CredentialFileIO();
    fileIO.setCredentials(
        List.of(
            StorageCredential.create(
                "s3://my-bucket/",
                Map.of(
                    IcebergVendedCredentialUtil.ACCESS_KEY_ID, "access",
                    IcebergVendedCredentialUtil.SECRET_ACCESS_KEY, "secret",
                    IcebergVendedCredentialUtil.ENDPOINT, "http://minio:9000"))));
    Table table =
        new BaseTable(new StaticTableOperations("s3://my-bucket/t", fileIO), "db.t");

    Configuration conf = new Configuration();
    conf.set("iceberg.catalog", "ice01");
    conf.set(
        "iceberg.catalog.ice01.header.X-Iceberg-Access-Delegation",
        "vended-credentials");
    conf.set("iceberg.catalog.ice01." + IcebergVendedCredentialUtil.ENDPOINT, "http://host:9000");

    IcebergVendedCredentialUtil.applyFromJobConf(table, conf);

    StorageCredential applied =
        ((SupportsStorageCredentials) table.io()).credentials().getFirst();
    assertThat(applied.prefix()).isEqualTo("s3://my-bucket/");
    assertThat(applied.config())
        .containsEntry(IcebergVendedCredentialUtil.ENDPOINT, "http://host:9000")
        .containsEntry(IcebergVendedCredentialUtil.ACCESS_KEY_ID, "access")
        .containsEntry(IcebergVendedCredentialUtil.SECRET_ACCESS_KEY, "secret");
  }

  /**
   * Tests {@link IcebergVendedCredentialUtil#applyFromJobConf} on a task with an empty FileIO.
   *
   * <p><b>Setup:</b> {@code propagateToJob} fills job maps on HS2; keys are copied into a task
   * {@code Configuration} as on Tez/LLAP; executor table is deserialized with a fresh FileIO (no
   * credentials).
   *
   * <p><b>Expects:</b> after apply, executor FileIO has one credential with the vended access key.
   *
   * <p><b>Why:</b> executors do not re-run REST {@code loadTable}; they rebuild the table from job
   * conf. {@link IcebergVendedCredentialUtil#resolveCredentialsForApply} must read the serialized
   * blob (or S3A secret keys) from the task conf and call {@code setCredentials} so Iceberg I/O
   * can access S3 during the task.
   */
  @Test
  public void applyFromJobConfRestoresCredentialsOnExecutor() {
    CredentialFileIO fileIO = new CredentialFileIO();
    fileIO.setCredentials(
        List.of(
            StorageCredential.create(
                "s3://my-bucket/",
                Map.of(IcebergVendedCredentialUtil.ACCESS_KEY_ID, "access",
                    IcebergVendedCredentialUtil.SECRET_ACCESS_KEY, "secret"))));
    Table table =
        new BaseTable(new StaticTableOperations("s3://my-bucket/t", fileIO), "db.t");

    Map<String, String> jobProps = Maps.newHashMap();
    Map<String, String> jobSecrets = Maps.newHashMap();
    IcebergVendedCredentialUtil.propagateToJob(
        table, "ice01", jobProps, jobSecrets, new Configuration());

    Configuration taskConf = new Configuration();
    jobProps.forEach(taskConf::set);
    jobSecrets.forEach(taskConf::set);
    taskConf.set(InputFormatConfig.CATALOG_NAME, "ice01");

    CredentialFileIO executorIo = new CredentialFileIO();
    Table executorTable =
        new BaseTable(new StaticTableOperations("s3://my-bucket/t", executorIo), "db.t");
    IcebergVendedCredentialUtil.applyFromJobConf(executorTable, taskConf);

    StorageCredential applied =
        ((SupportsStorageCredentials) executorTable.io()).credentials().getFirst();
    assertThat(((SupportsStorageCredentials) executorTable.io()).credentials()).hasSize(1);
    assertThat(applied.prefix()).isEqualTo("s3://my-bucket/");
    assertThat(applied.config())
        .containsEntry(IcebergVendedCredentialUtil.ACCESS_KEY_ID, "access")
        .containsEntry(IcebergVendedCredentialUtil.SECRET_ACCESS_KEY, "secret");
  }

  private static final class CredentialFileIO implements FileIO, SupportsStorageCredentials {
    private List<StorageCredential> credentials = List.of();
    private Map<String, String> properties = Map.of();

    @Override
    public void initialize(Map<String, String> props) {
      this.properties = props;
    }

    @Override
    public org.apache.iceberg.io.InputFile newInputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public org.apache.iceberg.io.OutputFile newOutputFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void deleteFile(String path) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, String> properties() {
      return properties;
    }

    @Override
    public void close() {
      // No-op: test stub does not hold resources.
    }

    @Override
    public List<StorageCredential> credentials() {
      return credentials;
    }

    @Override
    public void setCredentials(List<StorageCredential> creds) {
      this.credentials = creds;
    }
  }
}
