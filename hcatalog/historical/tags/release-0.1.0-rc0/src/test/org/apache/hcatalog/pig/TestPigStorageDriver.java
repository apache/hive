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
package org.apache.hcatalog.pig;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.apache.hcatalog.pig.HCatLoader;
import org.apache.hcatalog.pig.drivers.PigStorageInputDriver;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.UDFContext;
import org.apache.thrift.TException;

public class TestPigStorageDriver extends TestCase {

  private HiveConf howlConf;
  private Driver howlDriver;
  private HiveMetaStoreClient msc;

  @Override
  protected void setUp() throws Exception {

    howlConf = new HiveConf(this.getClass());
    howlConf.set(ConfVars.PREEXECHOOKS.varname, "");
    howlConf.set(ConfVars.POSTEXECHOOKS.varname, "");
    howlConf.set(ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    howlConf.set(ConfVars.SEMANTIC_ANALYZER_HOOK.varname, HCatSemanticAnalyzer.class.getName());
    howlDriver = new Driver(howlConf);
    msc = new HiveMetaStoreClient(howlConf);
    SessionState.start(new CliSessionState(howlConf));
    super.setUp();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
  }

  public void testPigStorageDriver() throws IOException{


    String fsLoc = howlConf.get("fs.default.name");
    Path tblPath = new Path(fsLoc, "/tmp/test_pig/data");
    String anyExistingFileInCurDir = "ivy.xml";
    tblPath.getFileSystem(howlConf).copyFromLocalFile(new Path(anyExistingFileInCurDir),tblPath);

    howlDriver.run("drop table junit_pigstorage");
    CommandProcessorResponse resp;
    String createTable = "create table junit_pigstorage (a string) partitioned by (b string) stored as RCFILE";

    resp = howlDriver.run(createTable);
    assertEquals(0, resp.getResponseCode());
    assertNull(resp.getErrorMessage());

    resp = howlDriver.run("alter table junit_pigstorage add partition (b='2010-10-10') location '"+new Path(fsLoc, "/tmp/test_pig")+"'");
    assertEquals(0, resp.getResponseCode());
    assertNull(resp.getErrorMessage());

    resp = howlDriver.run("alter table junit_pigstorage partition (b='2010-10-10') set fileformat inputformat '" + RCFileInputFormat.class.getName()
        +"' outputformat '"+RCFileOutputFormat.class.getName()+"' inputdriver '"+PigStorageInputDriver.class.getName()+"' outputdriver 'non-existent'");
    assertEquals(0, resp.getResponseCode());
    assertNull(resp.getErrorMessage());

    resp =  howlDriver.run("desc extended junit_pigstorage partition (b='2010-10-10')");
    assertEquals(0, resp.getResponseCode());
    assertNull(resp.getErrorMessage());

    PigServer server = new PigServer(ExecType.LOCAL, howlConf.getAllProperties());
    UDFContext.getUDFContext().setClientSystemProps();
    server.registerQuery(" a = load 'junit_pigstorage' using "+HCatLoader.class.getName()+";");
    Iterator<Tuple> itr = server.openIterator("a");
    DataInputStream stream = new DataInputStream(new BufferedInputStream(new FileInputStream(new File(anyExistingFileInCurDir))));
    while(itr.hasNext()){
      Tuple t = itr.next();
      assertEquals(2, t.size());
      if(t.get(0) != null) {
        // If underlying data-field is empty. PigStorage inserts null instead
        // of empty String objects.
        assertTrue(t.get(0) instanceof String);
        assertEquals(stream.readLine(), t.get(0));
      }
      else{
        assertTrue(stream.readLine().isEmpty());
      }
      assertTrue(t.get(1) instanceof String);

      assertEquals("2010-10-10", t.get(1));
    }
    assertEquals(0,stream.available());
    stream.close();
    howlDriver.run("drop table junit_pigstorage");
  }

  public void testDelim() throws MetaException, TException, UnknownTableException, NoSuchObjectException, InvalidOperationException, IOException{

    howlDriver.run("drop table junit_pigstorage_delim");

    CommandProcessorResponse resp;
    String createTable = "create table junit_pigstorage_delim (a string) partitioned by (b string) stored as RCFILE";

    resp = howlDriver.run(createTable);

    assertEquals(0, resp.getResponseCode());
    assertNull(resp.getErrorMessage());

    resp = howlDriver.run("alter table junit_pigstorage_delim add partition (b='2010-10-10')");
    assertEquals(0, resp.getResponseCode());
    assertNull(resp.getErrorMessage());

    resp = howlDriver.run("alter table junit_pigstorage_delim partition (b='2010-10-10') set fileformat inputformat '" + RCFileInputFormat.class.getName()
        +"' outputformat '"+RCFileOutputFormat.class.getName()+"' inputdriver '"+MyPigStorageDriver.class.getName()+"' outputdriver 'non-existent'");

    Partition part = msc.getPartition(MetaStoreUtils.DEFAULT_DATABASE_NAME, "junit_pigstorage_delim", "b=2010-10-10");
    Map<String,String> partParms = part.getParameters();
    partParms.put(PigStorageInputDriver.delim, "control-A");

    msc.alter_partition(MetaStoreUtils.DEFAULT_DATABASE_NAME, "junit_pigstorage_delim", part);

    PigServer server = new PigServer(ExecType.LOCAL, howlConf.getAllProperties());
    UDFContext.getUDFContext().setClientSystemProps();
    server.registerQuery(" a = load 'junit_pigstorage_delim' using "+HCatLoader.class.getName()+";");
    try{
      server.openIterator("a");
    }catch(FrontendException fe){}
  }
}
