package org.apache.hadoop.hive.ql.parse.repl.dump.io;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.same;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DBSerializerTest {
  @Mock
  private FileSystem fs;
  @Mock
  private Path writePath;

  @Test
  public void databaseNameIsInLowercase() throws IOException, SemanticException {
    DBSerializer dbSerializer =
        new DBSerializer(new Database("DBName", "", "", new HashMap<>()));
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    FSDataOutputStream stream = new FSDataOutputStream(out, null);
    when(fs.create(same(writePath))).thenReturn(stream);

    try (JsonWriter writer = new JsonWriter(fs, writePath)) {
      ReplicationSpec additionalPropertiesProvider = new ReplicationSpec();
      additionalPropertiesProvider.setCurrentReplicationState("34");
      dbSerializer.writeTo(writer, additionalPropertiesProvider);
    }
    String outputString = out.toString();
    assertTrue(outputString + " does not contain the database name in lowercase",
        outputString.contains("dbname"));
  }
}