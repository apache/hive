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

package org.apache.hadoop.hive.ql.metadata;

import java.io.IOException;
import java.net.ServerSocket;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.util.StringUtils;

/**
 *
 * TestHiveRemote.
 *
 * Tests using the Hive metadata class to make calls to a remote metastore
 */
public class TestHiveRemote extends TestHive {

  /**
   * Starts a remote metastore
   */
  private static class RunMS implements Runnable {
    String port;

    public RunMS(String port) {
      this.port = port;
    }

    @Override
    public void run() {
      try {
        HiveMetaStore.main(new String[] { port });
      } catch (Throwable e) {
        e.printStackTrace(System.err);
        assert false;
      }
    }

  }

  /**
   * Start a remote metastore and initialize a Hive object pointing at it.
   */
  @Override
  protected void setUp() throws Exception {
    super.setUp();
    hiveConf = new HiveConf(this.getClass());
    String port = findFreePort();
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + port);

    Thread t = new Thread(new RunMS(port));
    t.start();

    // Wait a little bit for the metastore to start.
    Thread.sleep(5000);


    try {
      hm = Hive.get(hiveConf);
    } catch (Exception e) {
      System.err.println(StringUtils.stringifyException(e));
      System.err
          .println("Unable to initialize Hive Metastore using configuration: \n "
          + hiveConf);
      throw e;
    }
  }

  /**
   * Finds a free port.
   *
   * @return a free port
   * @throws IOException
   */
  private String findFreePort() throws IOException {
    ServerSocket socket= new ServerSocket(0);
    int port = socket.getLocalPort();
    socket.close();
    return String.valueOf(port);
  }
}
