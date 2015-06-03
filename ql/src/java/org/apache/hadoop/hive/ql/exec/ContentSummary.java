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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ContentSummary extends org.apache.hadoop.fs.ContentSummary {

  private boolean validSummary;

  public ContentSummary() {}

  public ContentSummary(long length, long fileCount, long directoryCount, boolean validSummary) {
    super(length, fileCount, directoryCount);
    this.validSummary = validSummary;
  }

  public ContentSummary(org.apache.hadoop.fs.ContentSummary summary, boolean validSummary) {
    this(summary.getLength(), summary.getFileCount(), summary.getDirectoryCount(), validSummary);
  }

  public boolean isValidSummary() {
    return validSummary;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeBoolean(validSummary);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    validSummary = in.readBoolean();
  }
}
