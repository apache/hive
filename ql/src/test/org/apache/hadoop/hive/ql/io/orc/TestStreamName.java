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

package org.apache.hadoop.hive.ql.io.orc;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestStreamName {

  @Test
  public void test1() throws Exception {
    StreamName s1 = new StreamName(3, OrcProto.Stream.Kind.DATA);
    StreamName s2 = new StreamName(3,
        OrcProto.Stream.Kind.DICTIONARY_DATA);
    StreamName s3 = new StreamName(5, OrcProto.Stream.Kind.DATA);
    StreamName s4 = new StreamName(5,
        OrcProto.Stream.Kind.DICTIONARY_DATA);
    StreamName s1p = new StreamName(3, OrcProto.Stream.Kind.DATA);
    assertEquals(true, s1.equals(s1));
    assertEquals(false, s1.equals(s2));
    assertEquals(false, s1.equals(s3));
    assertEquals(true, s1.equals(s1p));
    assertEquals(true, s1.compareTo(null) < 0);
    assertEquals(false, s1.equals(null));
    assertEquals(true, s1.compareTo(s2) < 0);
    assertEquals(true, s2.compareTo(s3) < 0);
    assertEquals(true, s3.compareTo(s4) < 0);
    assertEquals(true, s4.compareTo(s1p) > 0);
    assertEquals(0, s1p.compareTo(s1));
  }
}
