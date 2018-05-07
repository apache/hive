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
package org.apache.hadoop.hive.ql.parse.authorization;

import java.util.List;

import junit.framework.Assert;

public class ListSizeMatcher<E> {
  private final List<E> list;
  private ListSizeMatcher(List<E> list) {
    this.list = list;
  }

  public List<E> ofSize(int size) {
    Assert.assertEquals(list.toString(),  size, list.size());
    return list;
  }


  public static <E> ListSizeMatcher<E> inList(List<E> list) {
    return new ListSizeMatcher<E>(list);
  }
}

