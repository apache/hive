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

package org.apache.hive.minikdc;

import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.BeforeClass;

public class TestJdbcWithMiniKdcSQLAuthBinary extends JdbcWithMiniKdcSQLAuthTest {

  @BeforeClass
  public static void beforeTest() throws Exception {
    hiveConf.setVar(ConfVars.HIVE_SERVER2_TRANSPORT_MODE, MiniHS2.HS2_HTTP_MODE);
    JdbcWithMiniKdcSQLAuthTest.beforeTestBase();

  }

}
