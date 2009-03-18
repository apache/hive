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

package org.apache.hadoop.hive.service;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.plan.tableDesc;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.service.ThriftHive;
import org.apache.hadoop.hive.service.ThriftHive.*;
import org.apache.hadoop.hive.service.HiveServerException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import com.facebook.fb303.FacebookBase;
import com.facebook.fb303.FacebookService;
import com.facebook.fb303.fb_status;
import com.facebook.thrift.TException;
import com.facebook.thrift.protocol.TBinaryProtocol;
import com.facebook.thrift.server.TServer;
import com.facebook.thrift.server.TThreadPoolServer;
import com.facebook.thrift.transport.TServerSocket;
import com.facebook.thrift.transport.TServerTransport;
import com.facebook.thrift.transport.TTransportFactory;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.*;

/**
 * Thrift Hive Server Implementation
 */
public class HiveServer extends ThriftHive {
  private final static String VERSION = "0";

  /**
   * Handler which implements the Hive Interface
   * This class can be used in lieu of the HiveClient class
   * to get an embedded server
   */
  public static class HiveServerHandler extends HiveMetaStore.HMSHandler implements HiveInterface {

    /**
     * Hive server uses org.apache.hadoop.hive.ql.Driver for run() and 
     * getResults() methods.
     * TODO: There should be one Driver object per query statement executed
     * TODO: That will allow clients to run multiple queries simulteneously
     */
    private Driver driver;

    /**
     * Stores state per connection
     */
    private SessionState session;

    public static final Log LOG = LogFactory.getLog(HiveServer.class.getName());

    /**
     * A constructor.
     */
    public HiveServerHandler() throws MetaException {
      super(HiveServer.class.getName());
      session = new SessionState(new HiveConf(SessionState.class));
      SessionState.start(session);
      HiveConf conf = session.get().getConf();
      session.in = null;
      session.out = null;
      session.err = null;
      driver = new Driver();
    }

    /**
     * Executes a query.
     *
     * @param query HiveQL query to execute
     */
    public void execute(String query) throws HiveServerException, TException {
      HiveServerHandler.LOG.info("Running the query: " + query);
      int rc = 0;
      // TODO: driver.run should either return int or throw exception, not both.
      try {
        rc = driver.run(query);
      } catch (Exception e) {
        throw new HiveServerException("Error running query: " + e.toString());
      }
      if (rc != 0) {
        throw new HiveServerException("Query returned non-zero code: " + rc);
      }
    }

    /**
     * Return the schema of the query result
     */
    public String getSchema() throws HiveServerException, TException {
      try {
        return driver.getSchema();
      }
      catch (Exception e) {
        throw new HiveServerException("Unable to get schema: " + e.toString());
      }
    }

    /** 
     * Fetches the next row in a query result set.
     * 
     * @return the next row in a query result set. null if there is no more row to fetch.
     */
    public String fetchOne() throws HiveServerException, TException {
      driver.setMaxRows(1);
      Vector<String> result = new Vector<String>();
      if (driver.getResults(result)) {
        return result.get(0);
      }
      // TODO: Cannot return null here because thrift cannot handle nulls
      // TODO: Returning empty string for now. Need to figure out how to
      // TODO: return null in some other way
      return "";
    }

    /**
     * Fetches numRows rows.
     *
     * @param numRows Number of rows to fetch.
     * @return A list of rows. The size of the list is numRows if there are at least 
     *         numRows rows available to return. The size is smaller than numRows if
     *         there aren't enough rows. The list will be empty if there is no more 
     *         row to fetch or numRows == 0. 
     * @throws HiveServerException Invalid value for numRows (numRows < 0)
     */
    public List<String> fetchN(int numRows) throws HiveServerException, TException {
      if (numRows < 0) {
        throw new HiveServerException("Invalid argument for number of rows: " + numRows);
      } 
      Vector<String> result = new Vector<String>();
      driver.setMaxRows(numRows);
      driver.getResults(result);
      return result;
    }

    /**
     * Fetches all the rows in a result set.
     *
     * @return All the rows in a result set of a query executed using execute method.
     *
     * TODO: Currently the server buffers all the rows before returning them 
     * to the client. Decide whether the buffering should be done in the client.
     */
    public List<String> fetchAll() throws HiveServerException, TException {
      Vector<String> rows = new Vector<String>();
      Vector<String> result = new Vector<String>();
      while (driver.getResults(result)) {
        rows.addAll(result);
        result.clear();
      }
      return rows;
    }
    
    /**
     * Return the status of the server
     */
    @Override
    public int getStatus() {
      return 0;
    }

    /**
     * Return the version of the server software
     */
    @Override
    public String getVersion() {
      return VERSION;
    }
  }

  public static void main(String[] args) {
    try {
      int port = 10000;
      if (args.length >= 1) {
        port = Integer.parseInt(args[0]);
      }
      TServerTransport serverTransport = new TServerSocket(port);
      Iface handler = new HiveServerHandler();
      FacebookService.Processor processor = new ThriftHive.Processor(handler);
      TThreadPoolServer.Options options = new TThreadPoolServer.Options();
      TServer server = new TThreadPoolServer(processor, serverTransport,
          new TTransportFactory(), new TTransportFactory(),
          new TBinaryProtocol.Factory(), new TBinaryProtocol.Factory(), options);
      server.serve();
      HiveServerHandler.LOG.info("Started the new hive server on port " + port);
    } catch (Exception x) {
      x.printStackTrace();
    }
  }
}
