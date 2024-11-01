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
package org.apache.hive.service.cli.thrift;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hive.service.server.HiveServer2;

import java.io.IOException;
import java.net.ServerSocket;

import org.junit.AfterClass;

public abstract class AbstractThriftCLITest {
    protected static int port;
    protected static HiveServer2 hiveServer2;
    protected static HiveConf hiveConf;
    protected static String host = "localhost";
    protected static ThriftCLIServiceClient client;
    protected static String USERNAME = "anonymous";
    protected static String PASSWORD = "anonymous";
    protected static final int RETRY_COUNT = 10;

    /**
     * @throws java.lang.Exception
     */
    public static void initConf(Class<?> cls) throws Exception {
        // Find a free port
        port = findFreePort();
        hiveServer2 = new HiveServer2();
        hiveConf = new HiveConfForTest(cls);
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        stopHiveServer2();
    }

    public static void startHiveServer2WithConf(HiveConf hiveConf) throws Exception {
        Exception hs2Exception = null;
        boolean hs2Started = false;
        for(int tryCount = 0; (tryCount < RETRY_COUNT); tryCount++){
            try {
                hiveServer2 = new HiveServer2();
                hiveServer2.init(hiveConf);
                hiveServer2.start();
                Thread.sleep(5000);
                hs2Started = true;
                break;
            } catch (Exception e) {
                hs2Exception = e;
                hiveConf.setIntVar(hiveConf,
                        HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT, findFreePort());
                hiveConf.setIntVar(hiveConf,
                        HiveConf.ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT, findFreePort());
                hiveConf.setIntVar(hiveConf,
                        HiveConf.ConfVars.HIVE_SERVER2_WEBUI_PORT, findFreePortExcepting(
                        Integer.valueOf(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_PORT.getDefaultValue())));
            }
        }
        if (!hs2Started) {
            throw(hs2Exception);
        }
        System.out.println("HiveServer2 started on port " + port);
    }

    public static void stopHiveServer2() throws Exception {
        if (hiveServer2 != null) {
            hiveServer2.stop();
        }
    }

    public static int findFreePort() throws IOException {
        ServerSocket socket= new ServerSocket(0);
        int port = socket.getLocalPort();
        socket.close();
        return port;
    }

    public static int findFreePortExcepting(int portToExclude) throws IOException {
        try (ServerSocket socket1 = new ServerSocket(0); ServerSocket socket2 = new ServerSocket(0)) {
            if (socket1.getLocalPort() != portToExclude) {
                return socket1.getLocalPort();
            }
            // If we're here, then socket1.getLocalPort was the port to exclude
            // Since both sockets were open together at a point in time, we're
            // guaranteed that socket2.getLocalPort() is not the same.
            return socket2.getLocalPort();
        }
    }

}
