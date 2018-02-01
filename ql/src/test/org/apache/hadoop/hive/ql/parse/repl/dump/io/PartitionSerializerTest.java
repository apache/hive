package org.apache.hadoop.hive.ql.parse.repl.dump.io;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
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
public class PartitionSerializerTest {
  @Mock
  private FileSystem fs;
  @Mock
  private Path writePath;

  @Test
  public void tableNameAndDatabaseNameAreInLowerCase() throws IOException, SemanticException {
    Partition partition = new Partition(Collections.emptyList(), "DBName", "TABLENAME",
        Integer.MAX_VALUE, Integer.MAX_VALUE, new StorageDescriptor(), Collections.emptyMap());
    PartitionSerializer partitionSerializer = new PartitionSerializer(partition);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    FSDataOutputStream stream = new FSDataOutputStream(out, null);
    when(fs.create(same(writePath))).thenReturn(stream);

    try (JsonWriter writer = new JsonWriter(fs, writePath)) {
      writer.jsonGenerator.writeFieldName("PartitionField");
      partitionSerializer.writeTo(writer, new ReplicationSpec());
    }

    String outputString = out.toString();
    assertTrue(outputString + " does not contain the table name in lowercase",
        outputString.contains("tablename"));
    assertTrue(outputString + " does not contain the database name in lowercase",
        outputString.contains("dbname"));
  }
}