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

package org.apache.hadoop.hive.ql.exec.spark;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.OutputCollector;

import scala.Tuple2;

public class SparkCollector implements OutputCollector<BytesWritable, BytesWritable>, Serializable {
  private static final long serialVersionUID = 1L;

  private List<Tuple2<BytesWritable, BytesWritable>> result = new ArrayList<Tuple2<BytesWritable, BytesWritable>>();
  
  @Override
  public void collect(BytesWritable key, BytesWritable value) throws IOException {
    result.add(new Tuple2<BytesWritable, BytesWritable>(new BytesWritable(key.copyBytes()), new BytesWritable(value.copyBytes())));
  }
  
  public void clear() {
    result.clear();
  }

  public List<Tuple2<BytesWritable, BytesWritable>> getResult() {
    return result;
  }

}
