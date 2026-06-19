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

package org.apache.hadoop.hive.ql.exec;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.LineRecordReader.LineReader;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;

/**
 * TextRecordReader.
 *
 */
public class TextRecordReader implements RecordReader {

  private LineReader lineReader;
  private InputStream in;
  private Text row;
  private Configuration conf;

  public void initialize(InputStream in, Configuration conf, Properties tbl)
      throws IOException {
    lineReader = new LineReader(in, conf);
    this.in = in;
    this.conf = conf;
  }

  public Writable createRow() throws IOException {
    row = new Text();
    return row;
  }

  public int next(Writable row) throws IOException {
    if (lineReader == null) {
      return -1;
    }

    int bytesConsumed = lineReader.readLine((Text) row);

    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_SCRIPT_ESCAPE)) {
      return HiveUtils.unescapeText((Text) row);
    }
    return bytesConsumed;
  }

  public void close() throws IOException {
    if (in != null) {
      in.close();
    }
  }
}
