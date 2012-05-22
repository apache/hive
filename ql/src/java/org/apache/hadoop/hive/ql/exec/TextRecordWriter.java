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

package org.apache.hadoop.hive.ql.exec;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.conf.HiveConf;

/**
 * TextRecordWriter.
 *
 */
public class TextRecordWriter implements RecordWriter {

  private OutputStream out;
  private Configuration conf;

  public void initialize(OutputStream out, Configuration conf)
      throws IOException {
    this.out = out;
    this.conf = conf;
  }

  public void write(Writable row) throws IOException {
    Text text = (Text) row;
    Text escapeText = text;

    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVESCRIPTESCAPE)) {
      escapeText = HiveUtils.escapeText(text);
    }

    out.write(escapeText.getBytes(), 0, escapeText.getLength());
    out.write(Utilities.newLineCode);
  }

  public void close() throws IOException {
    out.flush();
    out.close();
  }
}
