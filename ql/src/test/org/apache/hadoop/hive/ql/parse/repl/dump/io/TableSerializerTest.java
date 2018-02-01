package org.apache.hadoop.hive.ql.parse.repl.dump.io;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.same;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TableSerializerTest {
  @Mock
  private FileSystem fs;
  @Mock
  private Path writePath;

  @Test
  public void testTableNameAndDatabaseNameIsInLowerCase() throws IOException, SemanticException {
    Table tableHandle = new Table();
    tableHandle.setTTable(
        new org.apache.hadoop.hive.metastore.api.Table("TableName", "dbName", "test",
            Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, new StorageDescriptor(),
            Collections.emptyList(), Collections.emptyMap(), "", "",
            ""));

    TableSerializer tableSerializer =
        new TableSerializer(tableHandle, null, new HiveConf());

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    FSDataOutputStream stream =
        new FSDataOutputStream(out, null);
    when(fs.create(same(writePath))).thenReturn(stream);

    try (JsonWriter writer = new JsonWriter(fs, writePath)) {
      tableSerializer.writeTo(writer, new ReplicationSpec());
    }
    String outputString = out.toString();
    assertTrue(outputString + " does not contain the table name in lowercase",
        outputString.contains("tablename"));
    assertTrue(outputString + " does not contain the database name in lowercase",
        outputString.contains("dbname"));
  }
}