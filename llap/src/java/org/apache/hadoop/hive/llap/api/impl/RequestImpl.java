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


package org.apache.hadoop.hive.llap.api.impl;

import java.util.List;

import org.apache.hadoop.mapred.InputSplit;

import org.apache.hadoop.hive.llap.api.Reader;
import org.apache.hadoop.hive.llap.api.Request;
import org.apache.hadoop.hive.llap.processor.Pool;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;

public class RequestImpl implements Request {
  private InputSplit split;
  private List<Integer> includedColumnIds = null; // null means all columns
  private SearchArgument sarg = null;
  private final Pool processorPool;

  public RequestImpl(Pool processorPool) {
    this.processorPool = processorPool;
  }

  public Reader submit() {
    ReaderImpl reader = new ReaderImpl();
    processorPool.enqueue(this, reader);
    return reader;
  }

  public Request setSplit(InputSplit split) {
    this.split = split;
    return this;
  }

  public InputSplit getSplit() {
    return split;
  }

  public Request setColumns(List<Integer> columnIds) {
    this.includedColumnIds = columnIds;
    return this;
  }

  public List<Integer> getColumns() {
    return includedColumnIds;
  }

  public Request setSarg(SearchArgument sarg) {
    this.sarg = sarg;
    return this;
  }

  public SearchArgument getSarg() {
    return sarg;
  }
}
