/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.mr.hive;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.stream.StreamSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.Ciphers;
import org.apache.iceberg.encryption.UnitestKMS;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.mr.hive.test.TestTables.TestTableType;
import org.apache.iceberg.mr.hive.test.utils.HiveIcebergStorageHandlerTestUtils;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import static org.apache.iceberg.Files.localInput;

public class TestHiveCatalogEncryption extends HiveIcebergStorageHandlerWithEngineBase {

  private static final String ENCRYPTION_PROPS = String.format(
      "TBLPROPERTIES ('%s'='%s', '%s'='3')",
      TableProperties.ENCRYPTION_TABLE_KEY, UnitestKMS.MASTER_KEY_NAME1,
      TableProperties.FORMAT_VERSION);

  @Parameters(name = "fileFormat={0}, catalog={1}, isVectorized={2}, formatVersion={3}")
  public static Collection<Object[]> parameters() {
    return HiveIcebergStorageHandlerWithEngineBase.getParameters(p ->
        p.testTableType() == TestTableType.HIVE_CATALOG &&
        p.formatVersion() == 3 && p.isVectorized() && p.fileFormat() == FileFormat.PARQUET);
  }

  @BeforeClass
  public static void beforeClass() {
    shell = HiveIcebergStorageHandlerTestUtils.shell(
        ImmutableMap.of("iceberg.catalog.default_iceberg.encryption.kms-impl", UnitestKMS.class.getCanonicalName()));
  }

  @Before
  public void configureEncryption() {
    shell.getHiveConf().set("iceberg.catalog.default_iceberg.encryption.kms-impl", UnitestKMS.class.getCanonicalName());
  }

  @Test
  public void testWriteAndReadEncryptedTable() {
    TableIdentifier identifier = TableIdentifier.of("default", "encrypted_v3_table");
    shell.executeStatement(String.format(
        "CREATE EXTERNAL TABLE %s (id bigint, data string) STORED BY iceberg %s",
        identifier.name(), ENCRYPTION_PROPS));

    // Insert initial set of rows
    shell.executeStatement(String.format(
        "INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')", identifier.name()));

    // Verify all rows were inserted correctly
    List<Object[]> rows = shell.executeStatement("SELECT * FROM " + identifier.name() + " ORDER BY id");
    Assert.assertEquals("Should have 4 rows after insert", 4, rows.size());
    Assert.assertEquals(1L, rows.get(0)[0]);
    Assert.assertEquals("a", rows.get(0)[1]);
    Assert.assertEquals(4L, rows.get(3)[0]);
    Assert.assertEquals("d", rows.get(3)[1]);

    // Perform the DELETE operation
    shell.executeStatement(String.format(
        "DELETE FROM %s WHERE id < 3", identifier.name()));

    // Verify that the rows were actually deleted
    List<Object[]> rowsAfterDelete = shell.executeStatement("SELECT * FROM " + identifier.name() + " ORDER BY id");
    Assert.assertEquals("Should have 2 rows remaining after delete", 2, rowsAfterDelete.size());
    Assert.assertEquals(3L, rowsAfterDelete.get(0)[0]);
    Assert.assertEquals("c", rowsAfterDelete.get(0)[1]);
    Assert.assertEquals(4L, rowsAfterDelete.get(1)[0]);
    Assert.assertEquals("d", rowsAfterDelete.get(1)[1]);
  }

  @Test
  public void testManifestEncryption() {
    TableIdentifier identifier = TableIdentifier.of("default", "manifest_check");
    shell.executeStatement(String.format(
        "CREATE EXTERNAL TABLE %s (id bigint) STORED BY iceberg %s",
        identifier.name(), ENCRYPTION_PROPS));
    shell.executeStatement("INSERT INTO " + identifier.name() + " VALUES (1)");

    Table table = testTables.loadTable(identifier);
    Assert.assertEquals(3, ((BaseTable) table).operations().current().formatVersion());

    table.currentSnapshot().allManifests(table.io()).forEach(manifest -> {
      try (SeekableInputStream stream = localInput(manifest.path()).newStream()) {
        byte[] magic = new byte[4];
        stream.read(magic);
        Assert.assertEquals("Should have GCM magic",
            Ciphers.GCM_STREAM_MAGIC_STRING, new String(magic, StandardCharsets.UTF_8));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Test
  public void testAppendTransaction() {
    TableIdentifier identifier = TableIdentifier.of("default", "encrypted_append_transaction");

    shell.executeStatement(String.format(
        "CREATE EXTERNAL TABLE %s (id bigint, data string) STORED BY iceberg %s",
        identifier.name(), ENCRYPTION_PROPS));

    shell.executeStatement(String.format(
        "INSERT INTO %s VALUES (1, 'a')", identifier.name()));

    Table table = testTables.loadTable(identifier);
    org.apache.iceberg.DataFile dataFile = table.newScan().planFiles().iterator().next().file();

    long initialFiles = StreamSupport.stream(table.newScan().planFiles().spliterator(), false).count();

    // Start a transaction and append the data file
    org.apache.iceberg.Transaction transaction = table.newTransaction();
    org.apache.iceberg.AppendFiles append = transaction.newAppend();
    append.appendFile(dataFile);
    append.commit();
    transaction.commitTransaction();

    table.refresh();
    long finalFiles = StreamSupport.stream(table.newScan().planFiles().spliterator(), false).count();

    Assert.assertEquals("Should have added 1 data file via transaction",
        initialFiles + 1, finalFiles);
  }

  @Test
  public void testConcurrentAppendTransactions() {
    TableIdentifier identifier = TableIdentifier.of("default", "encrypted_concurrent_append");

    shell.executeStatement(String.format(
        "CREATE EXTERNAL TABLE %s (id bigint, data string) STORED BY iceberg %s",
        identifier.name(), ENCRYPTION_PROPS));

    shell.executeStatement(String.format(
        "INSERT INTO %s VALUES (1, 'a')", identifier.name()));

    Table table = testTables.loadTable(identifier);
    org.apache.iceberg.DataFile dataFile = table.newScan().planFiles().iterator().next().file();

    long initialFiles = java.util.stream.StreamSupport
        .stream(table.newScan().planFiles().spliterator(), false).count();

    // Start the first transaction
    Transaction transaction = table.newTransaction();
    AppendFiles append = transaction.newAppend();
    append.appendFile(dataFile);

    // Concurrently append to the table before committing the first transaction
    // using a separate table instance load
    testTables.loadTable(identifier).newFastAppend().appendFile(dataFile).commit();

    // Now commit the first transaction
    append.commit();
    transaction.commitTransaction();

    table.refresh();
    long finalFiles = StreamSupport.stream(table.newScan().planFiles().spliterator(), false).count();

    Assert.assertEquals("Should have added 2 data files total via concurrent transactions",
        initialFiles + 2, finalFiles);
  }

  @Test
  public void testConcurrentReplaceTransactions() {
    TableIdentifier identifier = TableIdentifier.of("default", "encrypted_concurrent_replace");

    shell.executeStatement(String.format(
        "CREATE EXTERNAL TABLE %s (id bigint, data string) STORED BY iceberg %s",
        identifier.name(), ENCRYPTION_PROPS));

    shell.executeStatement(String.format(
        "INSERT INTO %s VALUES (1, 'a')", identifier.name()));

    Table table = testTables.loadTable(identifier);
    DataFile dataFile = table.newScan().planFiles().iterator().next().file();

    Transaction firstReplace = table.newTransaction();
    firstReplace.newOverwrite()
        .overwriteByRowFilter(Expressions.alwaysTrue())
        .addFile(dataFile)
        .commit();

    org.apache.iceberg.Transaction secondReplace = table.newTransaction();
    secondReplace.newOverwrite()
        .overwriteByRowFilter(Expressions.alwaysTrue())
        .addFile(dataFile)
        .commit();

    // Commit the first replacement
    firstReplace.commitTransaction();

    // Commit the second replacement
    secondReplace.commitTransaction();

    table.refresh();
    long finalFiles = StreamSupport.stream(table.newScan().planFiles().spliterator(), false).count();

    // Since both were full table replace/overwrites with a single file, the final count should be exactly 1
    Assert.assertEquals("Should only have 1 data file after concurrent replace transactions",
        1, finalFiles);
  }

  @Test
  public void testRefresh() {
    TableIdentifier identifier = TableIdentifier.of("default", "encrypted_refresh_table");

    shell.executeStatement(String.format(
        "CREATE EXTERNAL TABLE %s (id bigint, data string) STORED BY iceberg %s",
        identifier.name(), ENCRYPTION_PROPS));
    shell.executeStatement(String.format(
        "INSERT INTO %s VALUES (1, 'a')", identifier.name()));

    // Load the table and check current snapshot
    Table table = testTables.loadTable(identifier);
    long snapshotIdBefore = table.currentSnapshot().snapshotId();

    // Insert more data via Hive
    shell.executeStatement(String.format(
        "INSERT INTO %s VALUES (2, 'b')", identifier.name()));

    // Refresh the table object and verify the snapshot has updated
    table.refresh();
    long snapshotIdAfter = table.currentSnapshot().snapshotId();

    Assert.assertNotEquals("Snapshot ID should change after insert and refresh",
        snapshotIdBefore, snapshotIdAfter);
  }

  @Test
  public void testMetadataTamperproofing() throws IOException {
    TableIdentifier identifier = TableIdentifier.of("default", "encrypted_tamperproof_table");

    shell.executeStatement(String.format(
        "CREATE EXTERNAL TABLE %s (id bigint, data string) STORED BY iceberg %s",
        identifier.name(), ENCRYPTION_PROPS));
    shell.executeStatement(String.format(
        "INSERT INTO %s VALUES (1, 'a')", identifier.name()));
    shell.executeStatement(String.format(
        "INSERT INTO %s VALUES (2, 'b')", identifier.name()));

    Table table = testTables.loadTable(identifier);
    TableMetadata metadata = ((BaseTable) table).operations().current();

    Path currentMetadataPath = new Path(metadata.metadataFileLocation());
    Path previousMetadataPath = new Path(metadata.previousFiles().get(0).file());

    ChecksumFileSystem fs = (ChecksumFileSystem) FileSystem.newInstance(new Configuration());

    // Tamper: replace current metadata file with a previous version
    Path checksumFile = fs.getChecksumFile(currentMetadataPath);
    fs.delete(checksumFile, false);
    fs.delete(currentMetadataPath, false);
    fs.rename(previousMetadataPath, currentMetadataPath);

    AssertHelpers.assertThrows("Should detect metadata tampering",
        RuntimeException.class,
        "might have been modified. Hash of metadata loaded from storage differs from HMS-stored metadata hash",
        () -> testTables.loadTable(identifier));
  }

  @Test
  public void testKeyDelete() {
    TableIdentifier identifier = TableIdentifier.of("default", "encrypted_key_delete_table");

    shell.executeStatement(String.format(
        "CREATE EXTERNAL TABLE %s (id bigint) STORED BY iceberg %s",
        identifier.name(), ENCRYPTION_PROPS));

    AssertHelpers.assertThrows("Should not allow removing encryption key",
        IllegalArgumentException.class,
        "Cannot remove key ID from an encrypted table",
        () -> shell.executeStatement(String.format(
            "ALTER TABLE %s UNSET TBLPROPERTIES ('%s')",
            identifier.name(), TableProperties.ENCRYPTION_TABLE_KEY)));
  }

  @Test
  public void testKeyAlter() {
    TableIdentifier identifier = TableIdentifier.of("default", "encrypted_key_alter_table");

    shell.executeStatement(String.format(
        "CREATE EXTERNAL TABLE %s (id bigint) STORED BY iceberg %s",
        identifier.name(), ENCRYPTION_PROPS));

    AssertHelpers.assertThrows("Should not allow modifying encryption key",
        IllegalArgumentException.class,
        "Cannot modify key ID of an encrypted table",
        () -> shell.executeStatement(String.format(
            "ALTER TABLE %s SET TBLPROPERTIES ('%s'='abcd')",
            identifier.name(), TableProperties.ENCRYPTION_TABLE_KEY)));
  }

  @Test
  public void testDirectDataFileRead() {
    TableIdentifier identifier = TableIdentifier.of("default", "encrypted_direct_read_table");

    shell.executeStatement(String.format(
        "CREATE EXTERNAL TABLE %s (id int, data string) STORED BY iceberg %s",
        identifier.name(), ENCRYPTION_PROPS));
    shell.executeStatement(String.format(
        "INSERT INTO %s VALUES (1, 'a')", identifier.name()));

    // Load table and extract data files via the Iceberg Java API
    Table table = testTables.loadTable(identifier);
    List<DataFile> dataFiles = Lists.newArrayList();
    table.newScan().planFiles().forEach(task -> dataFiles.add(task.file()));

    Assert.assertFalse("Data files should not be empty", dataFiles.isEmpty());

    Schema schema = new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()));

    // Attempt to read the encrypted Parquet files directly without keys
    for (DataFile dataFile : dataFiles) {
      try {
        Parquet.read(localInput(dataFile.path().toString()))
            .project(schema)
            .callInit()
            .build()
            .iterator()
            .next();
        Assert.fail("Should have thrown an exception when reading encrypted file directly");
      } catch (Exception e) {
        Assert.assertTrue("Exception should be related to encrypted footer",
            e instanceof ParquetCryptoRuntimeException ||
            String.valueOf(e).contains("Trying to read file with encrypted footer. No keys available"));
      }
    }
  }
}
