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

import java.io.IOException;
import java.util.List;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.service.ThriftHive;
import org.apache.hadoop.hive.service.HiveServerException;
import org.apache.hadoop.hive.ql.session.SessionState;

import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;

import org.apache.hadoop.hive.ql.plan.api.Query;
import org.apache.hadoop.hive.ql.plan.api.QueryPlan;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.*;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobTracker;

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
     */
    private Driver driver;

    /**
     * Stores state per connection
     */
    private SessionState session;
    
    /**
     * Flag that indicates whether the last executed command was a Hive query
     */
    private boolean isHiveQuery;

    public static final Log LOG = LogFactory.getLog(HiveServer.class.getName());

    /**
     * A constructor.
     */
    public HiveServerHandler() throws MetaException {
      super(HiveServer.class.getName());

      isHiveQuery = false;
      SessionState session = new SessionState(new HiveConf(SessionState.class));
      SessionState.start(session);
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
    public void execute(String cmd) throws HiveServerException, TException {
      HiveServerHandler.LOG.info("Running the query: " + cmd);
      SessionState ss = SessionState.get();
      
      String cmd_trimmed = cmd.trim();
      String[] tokens = cmd_trimmed.split("\\s");
      String cmd_1 = cmd_trimmed.substring(tokens[0].length()).trim();
      
      int ret = 0;
      try {
        CommandProcessor proc = CommandProcessorFactory.get(tokens[0]);
        if(proc != null) {
          if (proc instanceof Driver) {
        	  isHiveQuery = true;
            ret = driver.run(cmd);
          } else {
        	  isHiveQuery = false;
            ret = proc.run(cmd_1);
          }
        }
      } catch (Exception e) {
        throw new HiveServerException("Error running query: " + e.toString());
      }

      if (ret != 0) {
        throw new HiveServerException("Query returned non-zero code: " + ret);
      }
    }

    /**
     * Return the status information about the Map-Reduce cluster
     */
    public HiveClusterStatus getClusterStatus() throws HiveServerException, TException {
      HiveClusterStatus hcs;
      try {
        ClusterStatus cs = driver.getClusterStatus();
        JobTracker.State jbs = cs.getJobTrackerState();
        
        // Convert the ClusterStatus to its Thrift equivalent: HiveClusterStatus
        int state;
        switch (jbs) {
          case INITIALIZING:
            state = JobTrackerState.INITIALIZING;
            break;
          case RUNNING:
            state = JobTrackerState.RUNNING;
            break;
          default:
            String errorMsg = "Unrecognized JobTracker state: " + jbs.toString();
            throw new Exception(errorMsg);
        }
        
        hcs = new HiveClusterStatus(
            cs.getTaskTrackers(),
            cs.getMapTasks(),
            cs.getReduceTasks(),
            cs.getMaxMapTasks(),
            cs.getMaxReduceTasks(),
            state);
      }
      catch (Exception e) {
        LOG.error(e.toString());
        e.printStackTrace();
        throw new HiveServerException("Unable to get cluster status: " + e.toString());
      }
      return hcs;
    }
    
    /**
     * Return the Hive schema of the query result
     */
    public Schema getSchema() throws HiveServerException, TException {
      if (!isHiveQuery)
        // Return empty schema if the last command was not a Hive query
        return new Schema();	
    	
      try {
        Schema schema = driver.getSchema();
        if (schema == null) {
          schema = new Schema();
        }
        LOG.info("Returning schema: " + schema);
        return schema;
      }
      catch (Exception e) {
        LOG.error(e.toString());
        e.printStackTrace();
        throw new HiveServerException("Unable to get schema: " + e.toString());
      }
    }
    
    /**
     * Return the Thrift schema of the query result
     */
    public Schema getThriftSchema() throws HiveServerException, TException {
      if (!isHiveQuery)
        // Return empty schema if the last command was not a Hive query
        return new Schema();
    	
      try {
        Schema schema = driver.getThriftSchema();
        if (schema == null) {
          schema = new Schema();
        }
        LOG.info("Returning schema: " + schema);
        return schema;
      }
      catch (Exception e) {
        LOG.error(e.toString());
        e.printStackTrace();
        throw new HiveServerException("Unable to get schema: " + e.toString());
      }
    }
    
    
    /** 
     * Fetches the next row in a query result set.
     * 
     * @return the next row in a query result set. null if there is no more row to fetch.
     */
    public String fetchOne() throws HiveServerException, TException {
      if (!isHiveQuery)
        // Return no results if the last command was not a Hive query
        return "";
      
      Vector<String> result = new Vector<String>();
      driver.setMaxRows(1);
      try {
        if (driver.getResults(result)) {
          return result.get(0);
        }
        // TODO: Cannot return null here because thrift cannot handle nulls
        // TODO: Returning empty string for now. Need to figure out how to
        // TODO: return null in some other way
        return "";
      } catch (IOException e) {
        throw new HiveServerException(e.getMessage());
      }
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
      if (!isHiveQuery)
      	// Return no results if the last command was not a Hive query
        return new Vector<String>();
      
      Vector<String> result = new Vector<String>();      
      driver.setMaxRows(numRows);
      try {
        driver.getResults(result);
      } catch (IOException e) {
        throw new HiveServerException(e.getMessage());
      }
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
      if (!isHiveQuery)
        // Return no results if the last command was not a Hive query
        return new Vector<String>();
      
      Vector<String> rows = new Vector<String>();
      Vector<String> result = new Vector<String>();
      try {
        while (driver.getResults(result)) {
          rows.addAll(result);
          result.clear();
        }
      } catch (IOException e) {
        throw new HiveServerException(e.getMessage());
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

    @Override
    public QueryPlan getQueryPlan() throws HiveServerException, TException {
      QueryPlan qp = new QueryPlan();
      // TODO for now only return one query at a time
      // going forward, all queries associated with a single statement
      // will be returned in a single QueryPlan
      try {
        qp.addToQueries(driver.getQueryPlan());
      }
      catch (Exception e) {
        throw new HiveServerException(e.toString());
      }
      return qp;
    }
    
  }
	
  public static class ThriftHiveProcessorFactory extends TProcessorFactory {
    public ThriftHiveProcessorFactory (TProcessor processor) {
      super(processor);
    }

    public TProcessor getProcessor(TTransport trans) {
      try {
        Iface handler = new HiveServerHandler();
        return new ThriftHive.Processor(handler);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static void main(String[] args) {
    try {
      int port = 10000;
      if (args.length >= 1) {
        port = Integer.parseInt(args[0]);
      }
      TServerTransport serverTransport = new TServerSocket(port);
      ThriftHiveProcessorFactory hfactory = new ThriftHiveProcessorFactory(null);
      TThreadPoolServer.Options options = new TThreadPoolServer.Options();
      TServer server = new TThreadPoolServer(hfactory, serverTransport,
          new TTransportFactory(), new TTransportFactory(),
          new TBinaryProtocol.Factory(), new TBinaryProtocol.Factory(), options);
      HiveServerHandler.LOG.info("Starting hive server on port " + port);
      server.serve();
    } catch (Exception x) {
      x.printStackTrace();
    }
  }
}
