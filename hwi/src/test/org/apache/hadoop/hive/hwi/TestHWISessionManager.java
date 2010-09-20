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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.hwi;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;

import junit.framework.TestCase;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.history.HiveHistoryViewer;

/**
 * TestHWISessionManager.
 *
 */
public class TestHWISessionManager extends TestCase {

  private static String tableName = "test_hwi_table";

  private final HiveConf conf;
  private final Path dataFilePath;
  private HWISessionManager hsm;

  public TestHWISessionManager(String name) {
    super(name);
    conf = new HiveConf(TestHWISessionManager.class);
    String dataFileDir = conf.get("test.data.files").replace('\\', '/')
        .replace("c:", "");
    dataFilePath = new Path(dataFileDir, "kv1.txt");
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    hsm = new HWISessionManager();
    Thread t = new Thread(hsm);
    t.start();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    hsm.setGoOn(false);
  }

  public final void testHiveDriver() throws Exception {
    // create a user
    HWIAuth user1 = new HWIAuth();
    user1.setUser("hadoop");
    user1.setGroups(new String[] {"hadoop"});

    // create two sessions for user
    HWISessionItem user1_item1 = hsm.createSession(user1, "session1");
    HWISessionItem user1_item2 = hsm.createSession(user1, "session2");

    // create second user
    HWIAuth user2 = new HWIAuth();
    user2.setUser("user2");
    user2.setGroups(new String[] {"user2"});

    // create one session for this user
    HWISessionItem user2_item1 = hsm.createSession(user2, "session1");

    // testing storage of sessions in HWISessionManager
    assertEquals(hsm.findAllSessionsForUser(user1).size(), 2);
    assertEquals(hsm.findAllSessionsForUser(user2).size(), 1);
    assertEquals(hsm.findAllSessionItems().size(), 3);

    user1_item1.addQuery("set hive.support.concurrency = false");
    user1_item2.addQuery("set hive.support.concurrency = false");
    user2_item1.addQuery("set hive.support.concurrency = false");

    HWISessionItem searchItem = hsm.findSessionItemByName(user1, "session1");
    assertEquals(searchItem, user1_item1);

    searchItem.addQuery("create table " + tableName
        + " (key int, value string)");
    searchItem.addQuery("describe " + tableName);
    searchItem.clientStart();

    // wait for the session manager to make the table. It is non blocking API.
    synchronized (searchItem.runnable) {
      while (searchItem.getStatus() != HWISessionItem.WebSessionItemStatus.READY) {
        searchItem.runnable.wait();
      }
    }
    ArrayList<Integer> zero = new ArrayList<Integer>();
    zero.add(0);
    zero.add(0);
    zero.add(0);
    zero.add(0);
    zero.add(0);
    ArrayList<Integer> zero3 = new ArrayList<Integer>();
    zero3.add(0);
    zero3.add(0);
    zero3.add(0);
    zero3.add(0);
    ArrayList<Integer> zero1 = new ArrayList<Integer>();
    zero1.add(0);
    assertEquals(zero, searchItem.getQueryRet());

    ArrayList<ArrayList<String>> searchBlockRes = searchItem.getResultBucket();

    String resLine = searchBlockRes.get(0).get(2);
    assertEquals(true, resLine.contains("key"));
    assertEquals(true, resLine.contains("int"));
    String resLine2 = searchBlockRes.get(0).get(3);
    assertEquals(true, resLine2.contains("value"));
    assertEquals(true, resLine2.contains("string"));

    // load data into table
    searchItem.clientRenew();
    searchItem.addQuery("load data local inpath '" + dataFilePath.toString()
        + "' into table " + tableName);
    searchItem.clientStart();
    while (searchItem.getStatus() != HWISessionItem.WebSessionItemStatus.READY) {
      Thread.sleep(1);
    }
    assertEquals(zero1, searchItem.getQueryRet());

    // start two queries simultaniously
    user1_item2.addQuery("select distinct(test_hwi_table.key) from "
        + tableName);
    user2_item1.addQuery("select distinct(test_hwi_table.key) from "
        + tableName);

    // set result files to compare results
    File tmpdir = new File("/tmp/" + System.getProperty("user.name") + "/");
    if (tmpdir.exists() && !tmpdir.isDirectory()) {
      throw new RuntimeException(tmpdir + " exists but is not a directory");
    }

    if (!tmpdir.exists()) {
      if (!tmpdir.mkdirs()) {
        throw new RuntimeException("Could not make scratch directory " + tmpdir);
      }
    }

    File result1 = new File(tmpdir, "user1_item2");
    File result2 = new File(tmpdir, "user2_item1");
    user1_item2.setResultFile(result1.toString());
    user2_item1.setResultFile(result2.toString());
    user1_item2.setSSIsSilent(true);
    user2_item1.setSSIsSilent(true);

    user1_item2.clientStart();
    user2_item1.clientStart();

    synchronized (user1_item2.runnable) {
      while (user1_item2.getStatus() != HWISessionItem.WebSessionItemStatus.READY) {
        user1_item2.runnable.wait();
      }
    }

    synchronized (user2_item1.runnable) {
      while (user2_item1.getStatus() != HWISessionItem.WebSessionItemStatus.READY) {
        user2_item1.runnable.wait();
      }
    }

    assertEquals(zero3, user1_item2.getQueryRet());
    assertEquals(zero3, user2_item1.getQueryRet());
    assertEquals(true, isFileContentEqual(result1, result2));

    // clean up the files
    result1.delete();
    result2.delete();

    // test a session renew/refresh
    user2_item1.clientRenew();
    user2_item1.addQuery("select distinct(test_hwi_table.key) from "
        + tableName);
    user2_item1.clientStart();

    synchronized (user2_item1.runnable) {
      while (user2_item1.getStatus() != HWISessionItem.WebSessionItemStatus.READY) {
        user2_item1.runnable.wait();
      }
    }

    // cleanup
    HWISessionItem cleanup = hsm.createSession(user1, "cleanup");
    cleanup.addQuery("set hive.support.concurrency = false");
    cleanup.addQuery("drop table " + tableName);
    cleanup.clientStart();

    synchronized (cleanup.runnable) {
      while (cleanup.getStatus() != HWISessionItem.WebSessionItemStatus.READY) {
        cleanup.runnable.wait();
      }
    }

    // test the history is non null object.
    HiveHistoryViewer hhv = cleanup.getHistoryViewer();
    assertNotNull(hhv);
    assertEquals(zero3, cleanup.getQueryRet());
  }

  public boolean isFileContentEqual(File one, File two) throws Exception {
    if (one.exists() && two.exists()) {
      if (one.isFile() && two.isFile()) {
        if (one.length() == two.length()) {
          BufferedReader br1 = new BufferedReader(new FileReader(one));
          BufferedReader br2 = new BufferedReader(new FileReader(one));
          String line1 = null;
          String line2 = null;
          while ((line1 = br1.readLine()) != null) {
            line2 = br2.readLine();
            if (!line1.equals(line2)) {
              br1.close();
              br2.close();
              return false;
            }
          }
          br1.close();
          br2.close();
          return true;
        }
      }
    }
    return false;
  }
}
