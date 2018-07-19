/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.io.arrow;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ArrowWrapperWritable implements WritableComparable {
  private VectorSchemaRoot vectorSchemaRoot;

  public ArrowWrapperWritable(VectorSchemaRoot vectorSchemaRoot) {
    this.vectorSchemaRoot = vectorSchemaRoot;
  }
  public ArrowWrapperWritable() {}

  public VectorSchemaRoot getVectorSchemaRoot() {
    return vectorSchemaRoot;
  }

  public void setVectorSchemaRoot(VectorSchemaRoot vectorSchemaRoot) {
    this.vectorSchemaRoot = vectorSchemaRoot;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override public int compareTo(Object o) {
    return 0;
  }

  @Override public boolean equals(Object o) {
    return true;
  }
}
