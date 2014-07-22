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
package org.apache.hadoop.hive.serde2.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * ByteWritable.
 * This class was created before the Hadoop version of this class was available, and needs to
 * be kept around for backward compatibility of third-party UDFs/SerDes. We should consider
 * removing this class in favor of directly using the Hadoop one in the next major release.
 */
public class ByteWritable extends org.apache.hadoop.io.ByteWritable {

  public ByteWritable(byte b) {
    super(b);
  }

  public ByteWritable() {
    super();
  }

  static { // register this comparator
    WritableComparator.define(ByteWritable.class, new Comparator());
  }

}
