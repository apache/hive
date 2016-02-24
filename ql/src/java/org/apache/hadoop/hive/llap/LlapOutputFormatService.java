/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.llap;

import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.InputStream;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 *
 */
public class LlapOutputFormatService {

  private static final Logger LOG = LoggerFactory.getLogger(LlapOutputFormat.class);

  private static LlapOutputFormatService service;
  private final Map<String, RecordWriter> writers;
  private final ServerSocket socket;
  private final HiveConf conf;
  private final ExecutorService executor;
  private static final int WAIT_TIME = 5;

  private LlapOutputFormatService() throws IOException {
    writers = new HashMap<String, RecordWriter>();
    conf = new HiveConf();
    executor = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("LLAP output %d").build());
    socket = new ServerSocket(
      conf.getIntVar(HiveConf.ConfVars.LLAP_DAEMON_OUTPUT_SERVICE_PORT));
  }

  public static LlapOutputFormatService get() throws IOException {
    if (service == null) {
      service = new LlapOutputFormatService();
      service.start();
    }
    return service;
  }

  public void start() throws IOException {
    executor.submit(new Runnable() {
      byte[] buffer = new byte[4096];
      @Override
      public void run() {
	while (true) {
	  Socket s = null;
	  try {
	    s = socket.accept();
	    String id = readId(s);
	    LOG.debug("Received: "+id);
	    registerReader(s, id);
	  } catch (IOException io) {
	    if (s != null) {
	      try{
		s.close();
	      } catch (IOException io2) {
		// ignore
	      }
	    }
	  }
	}
      }

    private String readId(Socket s) throws IOException {
      InputStream in = s.getInputStream();
      int idx = 0;
      while((buffer[idx++] = (byte)in.read()) != '\0') {}
      return new String(buffer,0,idx-1);
    }

    private void registerReader(Socket s, String id) throws IOException {
      synchronized(service) {
	LOG.debug("registering socket for: "+id);
	LlapRecordWriter writer = new LlapRecordWriter(s.getOutputStream());
        writers.put(id, writer);
        service.notifyAll();
      }
    }
    }
    );
  }

  public void stop() throws IOException, InterruptedException {
    executor.shutdown();
    executor.awaitTermination(WAIT_TIME, TimeUnit.SECONDS);
    socket.close();
  }

  public <K,V> RecordWriter<K, V> getWriter(String id) throws IOException, InterruptedException {
    RecordWriter writer = null;
    synchronized(service) {
      while ((writer = writers.get(id)) == null) {
	LOG.debug("Waiting for writer for: "+id);
	service.wait();
      }
    }
    return writer;
  }
}
