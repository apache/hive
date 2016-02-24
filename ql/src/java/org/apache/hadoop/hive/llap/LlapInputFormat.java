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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import javax.security.auth.login.LoginException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.tez.TezProcessor;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TaskSpecBuilder;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Socket;

import org.apache.hadoop.hive.ql.exec.SerializationUtilities;

import com.esotericsoftware.kryo.Kryo;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.InputStream;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.io.FileNotFoundException;
import java.util.UUID;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.SplitLocationInfo;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.runtime.api.Event;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.tez.DagUtils;
import org.apache.hadoop.hive.ql.exec.tez.HiveSplitGenerator;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.mapreduce.hadoop.InputSplitInfoMem;
import org.apache.tez.mapreduce.hadoop.MRInputHelpers;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRInputUserPayloadProto;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitProto;
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitsProto;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputInitializer;
import org.apache.tez.runtime.api.InputInitializerContext;
import org.apache.tez.runtime.api.InputSpecUpdate;
import org.apache.tez.runtime.api.events.InputConfigureVertexTasksEvent;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.apache.tez.runtime.api.events.InputInitializerEvent;

import com.google.common.base.Preconditions;

public class LlapInputFormat<V extends WritableComparable> implements InputFormat<NullWritable, V> {

  private static final Logger LOG = LoggerFactory.getLogger(LlapInputFormat.class);

  private final TezWork work;
  private final Schema schema;

  public LlapInputFormat(TezWork tezWork, Schema schema) {
    this.work = tezWork;
    this.schema = schema;
  }

  // need empty constructor for bean instantiation
  public LlapInputFormat() {
    // None of these fields should be required during getRecordReader,
    // and should not be read.
    work = null;
    schema = null;
  }

  /*
   * This proxy record reader has the duty of establishing a connected socket with LLAP, then fire
   * off the work in the split to LLAP and finally return the connected socket back in an
   * LlapRecordReader. The LlapRecordReader class reads the results from the socket.
   */
  @Override
  public RecordReader<NullWritable, V> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {

    // Calls a static method to ensure none of the object fields are read.
    return _getRecordReader(split, job, reporter);
  }

  private static RecordReader _getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws
      IOException {
    LlapInputSplit llapSplit = (LlapInputSplit)split;

    // TODO: push event into LLAP

    // this is just the portion that sets up the io to receive data
    String host = split.getLocations()[0];

    // TODO: need to construct id here. Format is queryId + "_" + taskIndex
    String id = "foobar";

    HiveConf conf = new HiveConf();
    Socket socket = new Socket(host,
        conf.getIntVar(HiveConf.ConfVars.LLAP_DAEMON_OUTPUT_SERVICE_PORT));

    LOG.debug("Socket connected");

    socket.getOutputStream().write(id.getBytes());
    socket.getOutputStream().write(0);
    socket.getOutputStream().flush();

    LOG.debug("Registered id: " + id);

    return new LlapRecordReader(socket.getInputStream(), llapSplit.getSchema(), Text.class);
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    throw new IOException("These are not the splits you are looking for.");
  }
}
