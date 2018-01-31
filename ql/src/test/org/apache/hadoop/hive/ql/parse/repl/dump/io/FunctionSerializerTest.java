package org.apache.hadoop.hive.ql.parse.repl.dump.io;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.*;
import static org.mockito.Matchers.same;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(MockitoJUnitRunner.class)
public class FunctionSerializerTest {
  @Mock
  private FileSystem fs;
  @Mock
  private Path writePath;

  @Test
  public void databaseNameAndFunctionNameAreInLowerCase() throws IOException,
      SemanticException {
    Function function = new Function("TESTFUNCTION", "DBNAME",
        "org.apache.some.class.Clazz", "test",
        PrincipalType.USER, Integer.MAX_VALUE, FunctionType.JAVA, Collections.emptyList());
    FunctionSerializer functionSerializer = new FunctionSerializer(function, new HiveConf());

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    FSDataOutputStream stream = new FSDataOutputStream(out, null);
    when(fs.create(same(writePath))).thenReturn(stream);

    try (JsonWriter writer = new JsonWriter(fs, writePath)) {
      functionSerializer.writeTo(writer, new ReplicationSpec());
    }

    String outputString = out.toString();
    assertTrue(outputString + " does not contain the function name in lowercase",
        outputString.contains("testfunction"));
    assertTrue(outputString + " does not contain the database name in lowercase",
        outputString.contains("dbname"));
  }
}