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
package org.apache.hadoop.hive.ql.io.orc.encoded;

import org.apache.hadoop.hive.common.DiskRangeInfo;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.orc.impl.InStream;

public class SettableUncompressedStream extends InStream.UncompressedStream {

  public SettableUncompressedStream(String name, DiskRangeList input, long length) {
    super(name, input, 0, length);
    setOffset(input);
  }

  public void setBuffers(DiskRangeInfo diskRangeList) {
    reset(diskRangeList.getDiskRanges(), diskRangeList.getTotalLength());
    setOffset(diskRangeList.getDiskRanges());
  }

  private void setOffset(DiskRangeList diskRangeList) {
    if (diskRangeList == null) {
      // Empty stream, make sure available() call returns 0
      currentOffset = 0;
      position = length;
    } else {
      currentOffset = diskRangeList.getOffset();
    }
  }
}
