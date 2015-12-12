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
package org.apache.orc.impl;

import java.util.List;

import org.apache.hadoop.hive.common.DiskRangeInfo;
import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.orc.impl.InStream;

/**
 * An uncompressed stream whose underlying byte buffer can be set.
 */
public class SettableUncompressedStream extends InStream.UncompressedStream {

  public SettableUncompressedStream(String name, List<DiskRange> input, long length) {
    super(name, input, length);
    setOffset(input);
  }

  public void setBuffers(DiskRangeInfo diskRangeInfo) {
    reset(diskRangeInfo.getDiskRanges(), diskRangeInfo.getTotalLength());
    setOffset(diskRangeInfo.getDiskRanges());
  }

  private void setOffset(List<DiskRange> list) {
    currentOffset = list.isEmpty() ? 0 : list.get(0).getOffset();
  }
}
