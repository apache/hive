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

package org.apache.hadoop.hive.ql.anon.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.util.List;

public class PageReader2 {

  private List<Writable> key;
  private List<Writable> value;
  private IKeyReader<?> reader;

  PageReader2(final Configuration configuration) {
    String keyType = configuration.get("btree.key.type");
    String valueType = configuration.get("btree.value.type");
  }

  public boolean read() {
    key.add(new IntWritable(1));
    value.add(new IntWritable(1));
    return true;
  }

  public List<Writable> getKey() {
    return key;
  }

  public List<Writable> getValue() {
    return value;
  }

}

interface IKeyReader<T> {
  T read();
}

class KeyReader implements IKeyReader<Integer> {
  KeyReader() {

  }

  @Override
  public Integer read() {
    return 0;
  }
}
