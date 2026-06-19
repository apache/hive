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

import java.io.IOException;

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
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.ql.io.StreamingOutputFormat;

import com.google.common.base.Preconditions;

public class LlapOutputFormat<K extends Writable, V extends Writable>
  implements OutputFormat<K, V>, StreamingOutputFormat {

  public static final String LLAP_OF_ID_KEY = "llap.of.id";

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
  }

  @Override
  public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress) throws IOException {
    if (!LlapProxy.isDaemon()) {
      throw new IOException("LlapOutputFormat can only be used inside Llap");
    }
    try {
      return LlapOutputFormatService.get().<K,V>getWriter(job.get(LLAP_OF_ID_KEY));
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }
}
