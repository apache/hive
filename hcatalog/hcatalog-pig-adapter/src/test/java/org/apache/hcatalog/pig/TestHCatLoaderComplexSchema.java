/**
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hcatalog.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @deprecated Use/modify {@link org.apache.hive.hcatalog.pig.TestHCatLoaderComplexSchema} instead
 */
public class TestHCatLoaderComplexSchema {

  //private static MiniCluster cluster = MiniCluster.buildCluster();
  private static Driver driver;
  //private static Properties props;
  private static final Logger LOG = LoggerFactory.getLogger(TestHCatLoaderComplexSchema.class);

  private void dropTable(String tablename) throws IOException, CommandNeedRetryException {
    driver.run("drop table " + tablename);
  }

  private void createTable(String tablename, String schema, String partitionedBy) throws IOException, CommandNeedRetryException {
    String createTable;
    createTable = "create table " + tablename + "(" + schema + ") ";
    if ((partitionedBy != null) && (!partitionedBy.trim().isEmpty())) {
      createTable = createTable + "partitioned by (" + partitionedBy + ") ";
    }
    createTable = createTable + "stored as RCFILE tblproperties('hcat.isd'='org.apache.hcatalog.rcfile.RCFileInputDriver'," +
      "'hcat.osd'='org.apache.hcatalog.rcfile.RCFileOutputDriver') ";
    LOG.info("Creating table:\n {}", createTable);
    CommandProcessorResponse result = driver.run(createTable);
    int retCode = result.getResponseCode();
    if (retCode != 0) {
      throw new IOException("Failed to create table. [" + createTable + "], return code from hive driver : [" + retCode + " " + result.getErrorMessage() + "]");
    }
  }

  private void createTable(String tablename, String schema) throws IOException, CommandNeedRetryException {
    createTable(tablename, schema, null);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    HiveConf hiveConf = new HiveConf(TestHCatLoaderComplexSchema.class);
    hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    driver = new Driver(hiveConf);
    SessionState.start(new CliSessionState(hiveConf));
    //props = new Properties();
    //props.setProperty("fs.default.name", cluster.getProperties().getProperty("fs.default.name"));

  }

  private static final TupleFactory tf = TupleFactory.getInstance();
  private static final BagFactory bf = BagFactory.getInstance();

  private Tuple t(Object... objects) {
    return tf.newTuple(Arrays.asList(objects));
  }

  private DataBag b(Tuple... objects) {
    return bf.newDefaultBag(Arrays.asList(objects));
  }

  /**
   * artificially complex nested schema to test nested schema conversion
   * @throws Exception
   */
  @Test
  public void testSyntheticComplexSchema() throws Exception {
    String pigSchema =
        "a: " +
        "(" +
        "aa: chararray, " +
        "ab: long, " +
        "ac: map[], " +
        "ad: { t: (ada: long) }, " +
        "ae: { t: (aea:long, aeb: ( aeba: chararray, aebb: long)) }," +
        "af: (afa: chararray, afb: long) " +
        ")," +
        "b: chararray, " +
        "c: long, " +
        "d:  { t: (da:long, db: ( dba: chararray, dbb: long), dc: { t: (dca: long) } ) } ";

    // with extra structs
    String tableSchema =
      "a struct<" +
        "aa: string, " +
        "ab: bigint, " +
        "ac: map<string, string>, " +
        "ad: array<struct<ada:bigint>>, " +
        "ae: array<struct<aea:bigint, aeb: struct<aeba: string, aebb: bigint>>>," +
        "af: struct<afa: string, afb: bigint> " +
        ">, " +
        "b string, " +
        "c bigint, " +
        "d array<struct<da: bigint, db: struct<dba:string, dbb:bigint>, dc: array<struct<dca: bigint>>>>";

    // without extra structs
    String tableSchema2 =
      "a struct<" +
        "aa: string, " +
        "ab: bigint, " +
        "ac: map<string, string>, " +
        "ad: array<bigint>, " +
        "ae: array<struct<aea:bigint, aeb: struct<aeba: string, aebb: bigint>>>," +
        "af: struct<afa: string, afb: bigint> " +
        ">, " +
        "b string, " +
        "c bigint, " +
        "d array<struct<da: bigint, db: struct<dba:string, dbb:bigint>, dc: array<bigint>>>";

    List<Tuple> data = new ArrayList<Tuple>();
    for (int i = 0; i < 10; i++) {
      Tuple t = t(
        t(
          "aa test",
          2l,
          new HashMap<String, String>() {
            {
              put("ac test1", "test 1");
              put("ac test2", "test 2");
            }
          },
          b(t(3l), t(4l)),
          b(t(5l, t("aeba test", 6l))),
          t("afa test", 7l)
        ),
        "b test",
        (long) i,
        b(t(8l, t("dba test", 9l), b(t(10l)))));

      data.add(t);
    }
    verifyWriteRead("testSyntheticComplexSchema", pigSchema, tableSchema, data, true);
    verifyWriteRead("testSyntheticComplexSchema", pigSchema, tableSchema, data, false);
    verifyWriteRead("testSyntheticComplexSchema2", pigSchema, tableSchema2, data, true);
    verifyWriteRead("testSyntheticComplexSchema2", pigSchema, tableSchema2, data, false);

  }

  private void verifyWriteRead(String tablename, String pigSchema, String tableSchema, List<Tuple> data, boolean provideSchemaToStorer)
    throws IOException, CommandNeedRetryException, ExecException, FrontendException {
    MockLoader.setData(tablename + "Input", data);
    try {
      createTable(tablename, tableSchema);
      PigServer server = new PigServer(ExecType.LOCAL);
      server.setBatchOn();
      server.registerQuery("A = load '" + tablename + "Input' using org.apache.hcatalog.pig.MockLoader() AS (" + pigSchema + ");");
      Schema dumpedASchema = server.dumpSchema("A");
      server.registerQuery("STORE A into '" + tablename + "' using org.apache.hcatalog.pig.HCatStorer("
        + (provideSchemaToStorer ? "'', '" + pigSchema + "'" : "")
        + ");");

      ExecJob execJob = server.executeBatch().get(0);
      if (!execJob.getStatistics().isSuccessful()) {
        throw new RuntimeException("Import failed", execJob.getException());
      }
      // test that schema was loaded correctly
      server.registerQuery("X = load '" + tablename + "' using org.apache.hcatalog.pig.HCatLoader();");
      server.dumpSchema("X");
      Iterator<Tuple> it = server.openIterator("X");
      int i = 0;
      while (it.hasNext()) {
        Tuple input = data.get(i++);
        Tuple output = it.next();
        compareTuples(input, output);
        LOG.info("tuple : {} ", output);
      }
      Schema dumpedXSchema = server.dumpSchema("X");

      Assert.assertEquals(
        "expected " + dumpedASchema + " but was " + dumpedXSchema + " (ignoring field names)",
        "",
        compareIgnoreFiledNames(dumpedASchema, dumpedXSchema));

    } finally {
      dropTable(tablename);
    }
  }
  private void compareTuples(Tuple t1, Tuple t2) throws ExecException {
    Assert.assertEquals("Tuple Sizes don't match", t1.size(), t2.size());
    for (int i = 0; i < t1.size(); i++) {
      Object f1 = t1.get(i);
      Object f2 = t2.get(i);
      Assert.assertNotNull("left", f1);
      Assert.assertNotNull("right", f2);
      String msg = "right: " + f1 + ", left: " + f2;
      Assert.assertEquals(msg, noOrder(f1.toString()), noOrder(f2.toString()));
    }
  }
  
  private String noOrder(String s) {
    char[] chars = s.toCharArray();
    Arrays.sort(chars);
    return new String(chars);
  }

  private String compareIgnoreFiledNames(Schema expected, Schema got) throws FrontendException {
    if (expected == null || got == null) {
      if (expected == got) {
        return "";
      } else {
        return "\nexpected " + expected + " got " + got;
      }
    }
    if (expected.size() != got.size()) {
      return "\nsize expected " + expected.size() + " (" + expected + ") got " + got.size() + " (" + got + ")";
    }
    String message = "";
    for (int i = 0; i < expected.size(); i++) {
      FieldSchema expectedField = expected.getField(i);
      FieldSchema gotField = got.getField(i);
      if (expectedField.type != gotField.type) {
        message += "\ntype expected " + expectedField.type + " (" + expectedField + ") got " + gotField.type + " (" + gotField + ")";
      } else {
        message += compareIgnoreFiledNames(expectedField.schema, gotField.schema);
      }
    }
    return message;
  }

  /**
   * tests that unnecessary tuples are drop while converting schema
   * (Pig requires Tuples in Bags)
   * @throws Exception
   */
  @Test
  public void testTupleInBagInTupleInBag() throws Exception {
    String pigSchema = "a: { b : ( c: { d: (i : long) } ) }";

    String tableSchema = "a array< array< bigint > >";

    List<Tuple> data = new ArrayList<Tuple>();
    data.add(t(b(t(b(t(100l), t(101l))), t(b(t(110l))))));
    data.add(t(b(t(b(t(200l))), t(b(t(210l))), t(b(t(220l))))));
    data.add(t(b(t(b(t(300l), t(301l))))));
    data.add(t(b(t(b(t(400l))), t(b(t(410l), t(411l), t(412l))))));


    verifyWriteRead("TupleInBagInTupleInBag1", pigSchema, tableSchema, data, true);
    verifyWriteRead("TupleInBagInTupleInBag2", pigSchema, tableSchema, data, false);

    // test that we don't drop the unnecessary tuple if the table has the corresponding Struct
    String tableSchema2 = "a array< struct< c: array< struct< i: bigint > > > >";

    verifyWriteRead("TupleInBagInTupleInBag3", pigSchema, tableSchema2, data, true);
    verifyWriteRead("TupleInBagInTupleInBag4", pigSchema, tableSchema2, data, false);

  }

  @Test
  public void testMapWithComplexData() throws Exception {
    String pigSchema = "a: long, b: map[]";
    String tableSchema = "a bigint, b map<string, struct<aa:bigint, ab:string>>";

    List<Tuple> data = new ArrayList<Tuple>();
    for (int i = 0; i < 10; i++) {
      Tuple t = t(
        (long) i,
        new HashMap<String, Object>() {
          {
            put("b test 1", t(1l, "test 1"));
            put("b test 2", t(2l, "test 2"));
          }
        });

      data.add(t);
    }
    verifyWriteRead("testMapWithComplexData", pigSchema, tableSchema, data, true);
    verifyWriteRead("testMapWithComplexData2", pigSchema, tableSchema, data, false);

  }
}
