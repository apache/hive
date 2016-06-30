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

package org.apache.hadoop.hive.llap;

import org.junit.Assert;
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.AfterClass;

import java.net.Socket;

import java.io.OutputStream;
import java.io.InputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.LlapBaseRecordReader.ReaderEvent;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.LlapOutputSocketInitMessage;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;

public class TestLlapOutputFormat {

  private static final Logger LOG = LoggerFactory.getLogger(TestLlapOutputFormat.class);

  private static LlapOutputFormatService service;

  @BeforeClass
  public static void setUp() throws Exception {
    LOG.debug("Setting up output service");
    Configuration conf = new Configuration();
    // Pick random avail port
    HiveConf.setIntVar(conf, HiveConf.ConfVars.LLAP_DAEMON_OUTPUT_SERVICE_PORT, 0);
    LlapOutputFormatService.initializeAndStart(conf, null);
    service = LlapOutputFormatService.get();
    LlapProxy.setDaemon(true);
    LOG.debug("Output service up");
  }

  @AfterClass
  public static void tearDown() throws IOException, InterruptedException {
    LOG.debug("Tearing down service");
    service.stop();
    LOG.debug("Tearing down complete");
  }

  @Test
  public void testValues() throws Exception {
    JobConf job = new JobConf();

    for (int k = 0; k < 5; ++k) {
      String id = "foobar" + k;
      job.set(LlapOutputFormat.LLAP_OF_ID_KEY, id);
      LlapOutputFormat format = new LlapOutputFormat();

      HiveConf conf = new HiveConf();
      Socket socket = new Socket("localhost", service.getPort());

      LOG.debug("Socket connected");

      OutputStream socketStream = socket.getOutputStream();
      LlapOutputSocketInitMessage.newBuilder()
        .setFragmentId(id).build().writeDelimitedTo(socketStream);
      socketStream.flush();

      Thread.sleep(3000);

      LOG.debug("Data written");

      RecordWriter<NullWritable, Text> writer = format.getRecordWriter(null, job, null, null);
      Text text = new Text();

      LOG.debug("Have record writer");

      for (int i = 0; i < 10; ++i) {
        text.set(""+i);
        writer.write(NullWritable.get(),text);
      }

      writer.close(null);

      InputStream in = socket.getInputStream();
      LlapBaseRecordReader reader = new LlapBaseRecordReader(
          in, null, Text.class, job, null, null);

      LOG.debug("Have record reader");

      // Send done event, which LlapRecordReader is expecting upon end of input
      reader.handleEvent(ReaderEvent.doneEvent());

      int count = 0;
      while(reader.next(NullWritable.get(), text)) {
        LOG.debug(text.toString());
        count++;
      }

      reader.close();

      Assert.assertEquals(10, count);
    }
  }


  @Test
  public void testBadClientMessage() throws Exception {
    JobConf job = new JobConf();
    String id = "foobar";
    job.set(LlapOutputFormat.LLAP_OF_ID_KEY, id);
    LlapOutputFormat format = new LlapOutputFormat();

    Socket socket = new Socket("localhost", service.getPort());

    LOG.debug("Socket connected");

    OutputStream socketStream = socket.getOutputStream();
    LlapOutputSocketInitMessage.newBuilder()
      .setFragmentId(id).build().writeDelimitedTo(socketStream);
    LlapOutputSocketInitMessage.newBuilder()
      .setFragmentId(id).build().writeDelimitedTo(socketStream);
    socketStream.flush();

    Thread.sleep(3000);

    LOG.debug("Data written");

    try {
      format.getRecordWriter(null, job, null, null);
      Assert.fail("Didn't throw");
    } catch (IOException ex) {
      // Expected.
    }
  }
}
