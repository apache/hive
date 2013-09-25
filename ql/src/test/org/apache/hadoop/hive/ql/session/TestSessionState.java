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
package org.apache.hadoop.hive.ql.session;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * Test SessionState
 */
public class TestSessionState {


  @Before
  public void setup(){
    SessionState.start(new HiveConf());
  }

  /**
   * test set and get db
   */
  @Test
  public void testgetDbName() throws Exception {
    //check that we start with default db
    assertEquals(MetaStoreUtils.DEFAULT_DATABASE_NAME,
        SessionState.get().getCurrentDatabase());
    final String newdb = "DB_2";

    //set new db and verify get
    SessionState.get().setCurrentDatabase(newdb);
    assertEquals(newdb,
        SessionState.get().getCurrentDatabase());

    //verify that a new sessionstate has default db
    SessionState.start(new HiveConf());
    assertEquals(MetaStoreUtils.DEFAULT_DATABASE_NAME,
        SessionState.get().getCurrentDatabase());

  }


}
