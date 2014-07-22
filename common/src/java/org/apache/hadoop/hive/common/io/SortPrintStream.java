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

package org.apache.hadoop.hive.common.io;

import com.google.common.collect.MinMaxPriorityQueue;

import java.io.OutputStream;
import java.util.Comparator;

public class SortPrintStream extends FetchConverter {

  private static final Comparator<String> STR_COMP = new Comparator<String>() {
    @Override
    public int compare(String o1, String o2) {
      return o1.compareTo(o2);
    }
  };

  protected final MinMaxPriorityQueue<String> outputs =
      MinMaxPriorityQueue.orderedBy(STR_COMP).create();

  public SortPrintStream(OutputStream out, String encoding) throws Exception {
    super(out, false, encoding);
  }

  @Override
  public void process(String out) {
    assert out != null;
    outputs.add(out);
  }

  @Override
  public void processFinal() {
    while (!outputs.isEmpty()) {
      printDirect(outputs.removeFirst());
    }
  }
}
