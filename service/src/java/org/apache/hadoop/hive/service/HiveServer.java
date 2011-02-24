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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.plan.api.QueryPlan;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorFactory;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobTracker;
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

import com.facebook.fb303.fb_status;

/**
 * Thrift Hive Server Implementation.
 */
public class HiveServer extends ThriftHive {
  private static final String VERSION = "1";

  /**
   * Handler which implements the Hive Interface This class can be used in lieu
   * of the HiveClient class to get an embedded server.
   */
  public static class HiveServerHandler extends HiveMetaStore.HMSHandler
      implements HiveInterface {
    /**
     * Hive server uses org.apache.hadoop.hive.ql.Driver for run() and
     * getResults() methods.
     * It is the instance of the last Hive query.
     */
    private Driver driver;
    /**
     * For processors other than Hive queries (Driver), they output to session.out (a temp file)
     * first and the fetchOne/fetchN/fetchAll functions get the output from pipeIn.
     */
    private BufferedReader pipeIn;

    /**
     * Flag that indicates whether the last executed command was a Hive query.
     */
    private boolean isHiveQuery;

    public static final Log LOG = LogFactory.getLog(HiveServer.class.getName());

    /**
     * A constructor.
     */
    public HiveServerHandler() throws MetaException {
      super(HiveServer.class.getName());

      isHiveQuery = false;
      driver = null;
      SessionState session = new SessionState(new HiveConf(SessionState.class));
      SessionState.start(session);
      setupSessionIO(session);
    }

    private void setupSessionIO(SessionState session) {
      try {
        LOG.info("Putting temp output to file " + session.getTmpOutputFile().toString());
        session.in = null; // hive server's session input stream is not used
        // open a per-session file in auto-flush mode for writing temp results
        session.out = new PrintStream(new FileOutputStream(session.getTmpOutputFile()), true, "UTF-8");
        // TODO: for hadoop jobs, progress is printed out to session.err,
        // we should find a way to feed back job progress to client
        session.err = new PrintStream(System.err, true, "UTF-8");
      } catch (IOException e) {
        LOG.error("Error in creating temp output file ", e);
        try {
          session.in = null;
          session.out = new PrintStream(System.out, true, "UTF-8");
          session.err = new PrintStream(System.err, true, "UTF-8");
      	  } catch (UnsupportedEncodingException ee) {
      	    ee.printStackTrace();
      	    session.out = null;
      	    session.err = null;
      	  }
      }
    }

    /**
     * Executes a query.
     *
     * @param cmd
     *          HiveQL query to execute
     */
    public void execute(String cmd) throws HiveServerException, TException {
      HiveServerHandler.LOG.info("Running the query: " + cmd);
      SessionState session = SessionState.get();

      String cmd_trimmed = cmd.trim();
      String[] tokens = cmd_trimmed.split("\\s");
      String cmd_1 = cmd_trimmed.substring(tokens[0].length()).trim();

      int ret = 0;
      String errorMessage = "";
      String SQLState = null;

      try {
        CommandProcessor proc = CommandProcessorFactory.get(tokens[0]);
        CommandProcessorResponse response = null;
        if (proc != null) {
          if (proc instanceof Driver) {
            isHiveQuery = true;
            driver = (Driver) proc;
            response = driver.run(cmd);
          } else {
            isHiveQuery = false;
            driver = null;
            // need to reset output for each non-Hive query
            setupSessionIO(session);
            response = proc.run(cmd_1);
          }

          ret = response.getResponseCode();
          SQLState = response.getSQLState();
          errorMessage = response.getErrorMessage();
        }
      } catch (Exception e) {
        HiveServerException ex = new HiveServerException();
        ex.setMessage("Error running query: " + e.toString());
        ex.setErrorCode(ret == 0? -10000: ret);
        throw ex;
      }

      if (ret != 0) {
        throw new HiveServerException("Query returned non-zero code: " + ret
            + ", cause: " + errorMessage, ret, SQLState);
      }
    }

    /**
     * Should be called by the client at the end of a session.
     */
    public void clean() {
      if (driver != null) {
        driver.close();
        driver.destroy();
      }

      SessionState session = SessionState.get();
      if (session.getTmpOutputFile() != null) {
        session.getTmpOutputFile().delete();
      }
      pipeIn = null;
    }

    /**
     * Return the status information about the Map-Reduce cluster.
     */
    public HiveClusterStatus getClusterStatus() throws HiveServerException,
        TException {
      HiveClusterStatus hcs;
      try {
        Driver drv = new Driver();
        drv.init();

        ClusterStatus cs = drv.getClusterStatus();
        JobTracker.State jbs = cs.getJobTrackerState();

        // Convert the ClusterStatus to its Thrift equivalent: HiveClusterStatus
        JobTrackerState state;
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

        hcs = new HiveClusterStatus(cs.getTaskTrackers(), cs.getMapTasks(), cs
            .getReduceTasks(), cs.getMaxMapTasks(), cs.getMaxReduceTasks(),
            state);
      } catch (Exception e) {
        LOG.error(e.toString());
        e.printStackTrace();
        HiveServerException ex = new HiveServerException();
        ex.setMessage("Unable to get cluster status: " + e.toString());
        throw ex;
      }
      return hcs;
    }

    /**
     * Return the Hive schema of the query result.
     */
    public Schema getSchema() throws HiveServerException, TException {
      if (!isHiveQuery) {
        // Return empty schema if the last command was not a Hive query
        return new Schema();
      }

      assert driver != null: "getSchema() is called on a Hive query and driver is NULL.";

      try {
        Schema schema = driver.getSchema();
        if (schema == null) {
          schema = new Schema();
        }
        LOG.info("Returning schema: " + schema);
        return schema;
      } catch (Exception e) {
        LOG.error(e.toString());
        e.printStackTrace();
        HiveServerException ex = new HiveServerException();
        ex.setMessage("Unable to get schema: " + e.toString());
        throw ex;
      }
    }

    /**
     * Return the Thrift schema of the query result.
     */
    public Schema getThriftSchema() throws HiveServerException, TException {
      if (!isHiveQuery) {
        // Return empty schema if the last command was not a Hive query
        return new Schema();
      }

      assert driver != null: "getThriftSchema() is called on a Hive query and driver is NULL.";

      try {
        Schema schema = driver.getThriftSchema();
        if (schema == null) {
          schema = new Schema();
        }
        LOG.info("Returning schema: " + schema);
        return schema;
      } catch (Exception e) {
        LOG.error(e.toString());
        e.printStackTrace();
        HiveServerException ex = new HiveServerException();
        ex.setMessage("Unable to get schema: " + e.toString());
        throw ex;
      }
    }


    /**
     * Fetches the next row in a query result set.
     *
     * @return the next row in a query result set. null if there is no more row
     *         to fetch.
     */
    public String fetchOne() throws HiveServerException, TException {
      if (!isHiveQuery) {
        // Return no results if the last command was not a Hive query
        List<String> results = new ArrayList<String>(1);
        readResults(results, 1);
        if (results.size() > 0) {
          return results.get(0);
        } else { //  throw an EOF exception
          throw new HiveServerException("OK", 0, "");
        }
      }

      assert driver != null: "fetchOne() is called on a Hive query and driver is NULL.";

      ArrayList<String> result = new ArrayList<String>();
      driver.setMaxRows(1);
      try {
        if (driver.getResults(result)) {
          return result.get(0);
        }
        // TODO: Cannot return null here because thrift cannot handle nulls
        // TODO: Returning empty string for now. Need to figure out how to
        // TODO: return null in some other way
        throw new HiveServerException("OK", 0, "");
       // return "";
      } catch (IOException e) {
        HiveServerException ex = new HiveServerException();
        ex.setMessage(e.getMessage());
        throw ex;
      }
    }

    private void cleanTmpFile() {
      if (pipeIn != null) {
        SessionState session = SessionState.get();
        File tmp = session.getTmpOutputFile();
        tmp.delete();
        pipeIn = null;
      }
    }

    /**
     * Reads the temporary results for non-Hive (non-Driver) commands to the
     * resulting List of strings.
     * @param results list of strings containing the results
     * @param nLines number of lines read at once. If it is <= 0, then read all lines.
     */
    private void readResults(List<String> results, int nLines) {

      if (pipeIn == null) {
        SessionState session = SessionState.get();
        File tmp = session.getTmpOutputFile();
        try {
          pipeIn = new BufferedReader(new FileReader(tmp));
        } catch (FileNotFoundException e) {
          LOG.error("File " + tmp + " not found. ", e);
          return;
        }
      }

      boolean readAll = false;

      for (int i = 0; i < nLines || nLines <= 0; ++i) {
        try {
          String line = pipeIn.readLine();
          if (line == null) {
            // reached the end of the result file
            readAll = true;
            break;
          } else {
            results.add(line);
          }
        } catch (IOException e) {
          LOG.error("Reading temp results encountered an exception: ", e);
          readAll = true;
        }
      }
      if (readAll) {
        cleanTmpFile();
      }
    }

    /**
     * Fetches numRows rows.
     *
     * @param numRows
     *          Number of rows to fetch.
     * @return A list of rows. The size of the list is numRows if there are at
     *         least numRows rows available to return. The size is smaller than
     *         numRows if there aren't enough rows. The list will be empty if
     *         there is no more row to fetch or numRows == 0.
     * @throws HiveServerException
     *           Invalid value for numRows (numRows < 0)
     */
    public List<String> fetchN(int numRows) throws HiveServerException,
        TException {
      if (numRows < 0) {
        HiveServerException ex = new HiveServerException();
        ex.setMessage("Invalid argument for number of rows: " + numRows);
        throw ex;
      }

      ArrayList<String> result = new ArrayList<String>();

      if (!isHiveQuery) {
        readResults(result, numRows);
        return result;
      }

      assert driver != null: "fetchN() is called on a Hive query and driver is NULL.";

      driver.setMaxRows(numRows);
      try {
        driver.getResults(result);
      } catch (IOException e) {
        HiveServerException ex = new HiveServerException();
        ex.setMessage(e.getMessage());
        throw ex;
      }
      return result;
    }

    /**
     * Fetches all the rows in a result set.
     *
     * @return All the rows in a result set of a query executed using execute
     *         method.
     *
     *         TODO: Currently the server buffers all the rows before returning
     *         them to the client. Decide whether the buffering should be done
     *         in the client.
     */
    public List<String> fetchAll() throws HiveServerException, TException {

      ArrayList<String> rows = new ArrayList<String>();
      ArrayList<String> result = new ArrayList<String>();

      if (!isHiveQuery) {
        // Return all results if numRows <= 0
        readResults(result, 0);
        return result;
      }

      try {
        while (driver.getResults(result)) {
          rows.addAll(result);
          result.clear();
        }
      } catch (IOException e) {
        HiveServerException ex = new HiveServerException();
        ex.setMessage(e.getMessage());
        throw ex;
      }
      return rows;
    }

    /**
     * Return the status of the server.
     */
    @Override
    public fb_status getStatus() {
      return fb_status.ALIVE;
    }

    /**
     * Return the version of the server software.
     */
    @Override
    public String getVersion() {
      return VERSION;
    }

    @Override
    public QueryPlan getQueryPlan() throws HiveServerException, TException {
      QueryPlan qp = new QueryPlan();

      if (!isHiveQuery) {
        return qp;
      }

      assert driver != null: "getQueryPlan() is called on a Hive query and driver is NULL.";

      // TODO for now only return one query at a time
      // going forward, all queries associated with a single statement
      // will be returned in a single QueryPlan
      try {
        qp.addToQueries(driver.getQueryPlan());
      } catch (Exception e) {
        HiveServerException ex = new HiveServerException();
        ex.setMessage(e.toString());
        throw ex;
      }
      return qp;
    }

  }

  /**
   * ThriftHiveProcessorFactory.
   *
   */
  public static class ThriftHiveProcessorFactory extends TProcessorFactory {
    public ThriftHiveProcessorFactory(TProcessor processor) {
      super(processor);
    }

    @Override
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
      int minWorkerThreads = 100; // default number of threads serving the Hive server
      if (args.length >= 1) {
        port = Integer.parseInt(args[0]);
      }
      if (args.length >= 2) {
        minWorkerThreads = Integer.parseInt(args[1]);
      }
      TServerTransport serverTransport = new TServerSocket(port);
      ThriftHiveProcessorFactory hfactory = new ThriftHiveProcessorFactory(null);
      TThreadPoolServer.Options options = new TThreadPoolServer.Options();
      options.minWorkerThreads = minWorkerThreads;
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
