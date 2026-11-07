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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.exec;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;

import org.apache.fory.Fory;
import org.apache.fory.config.Language;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.CopyOnFirstWriteProperties;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ArrayListMultimap;

/**
 * Evaluation-only tests: round-trip the types that {@link SerializationUtilities} registers
 * custom Kryo serializers for (see createNewKryo()) through Apache Fory instead, to surface
 * which custom (de)serialization logic would need to be reimplemented if Fory replaced Kryo.
 * Not wired into any production code path.
 */
public class TestFurySerialization {

  private Fory fory;

  @Before
  public void setup() {
    fory = Fory.builder()
        .withLanguage(Language.JAVA)
        .withRefTracking(true)
        .requireClassRegistration(false)
        .build();
  }

  private <T> T roundTrip(T value) {
    byte[] bytes = fory.serialize(value);
    return (T) fory.deserialize(bytes);
  }

  @Test
  public void testSqlDate() {
    Date date = Date.valueOf("2024-01-15");
    Assert.assertEquals(date, roundTrip(date));
  }

  @Test
  public void testSqlTimestamp() {
    Timestamp ts = new Timestamp(System.currentTimeMillis());
    ts.setNanos(123456789);
    Assert.assertEquals(ts, roundTrip(ts));
  }

  @Test
  public void testTimestampTZ() {
    TimestampTZ tz = new TimestampTZ(1700000000L, 123456789, ZoneId.of("America/Los_Angeles"));
    TimestampTZ result = roundTrip(tz);
    Assert.assertEquals(tz.getZonedDateTime(), result.getZonedDateTime());
  }

  @Test
  public void testHadoopPath() {
    Path path = new Path("/warehouse/test_table/p=0");
    Assert.assertEquals(path, roundTrip(path));
  }

  @Test
  public void testArraysAsList() {
    List<String> list = Arrays.asList("a", "b", "c");
    List<String> result = roundTrip(list);
    Assert.assertEquals(list, result);
  }

  @Test
  public void testArrayListSubList() {
    ArrayList<String> backing = new ArrayList<>(Arrays.asList("a", "b", "c", "d"));
    List<String> subList = backing.subList(1, 3);
    List<String> result = roundTrip(subList);
    Assert.assertEquals(subList, result);
  }

  @Test
  public void testCopyOnFirstWriteProperties() {
    Properties base = new Properties();
    base.setProperty("name", "test_table");
    CopyOnFirstWriteProperties props = new CopyOnFirstWriteProperties(base);
    props.setProperty("columns", "a,b,c");

    CopyOnFirstWriteProperties result = roundTrip(props);
    Assert.assertEquals("test_table", result.getProperty("name"));
    Assert.assertEquals("a,b,c", result.getProperty("columns"));
  }

  @Test
  public void testGuavaArrayListMultimap() {
    ArrayListMultimap<String, String> multimap = ArrayListMultimap.create();
    multimap.put("coltype", "foo");
    multimap.put("coltype", "bar");

    ArrayListMultimap<String, String> result = roundTrip(multimap);
    Assert.assertEquals(multimap.get("coltype"), result.get("coltype"));
  }

  @Test
  public void testMapWork() throws Exception {
    MapWork mapWork = mockMapWork();
    MapWork result = roundTrip(mapWork);
    Assert.assertEquals(
        mapWork.getPathToPartitionInfo().keySet(), result.getPathToPartitionInfo().keySet());
  }

  @Test
  public void testPartitionDesc() throws Exception {
    Properties tblProps = new Properties();
    TableDesc tbl = new TableDesc(OrcInputFormat.class, OrcOutputFormat.class, tblProps);
    PartitionDesc desc = new PartitionDesc(tbl, new LinkedHashMap<>());

    PartitionDesc result = roundTrip(desc);
    Assert.assertEquals(desc.getTableName(), result.getTableName());
  }

  /**
   * Probes whether Fory offers an equivalent to Kryo's root-type security guard
   * ({@code SerializationUtilities.KryoWithHooks#getRegistration}), which rejects a payload
   * whose first deserialized class doesn't match an expected type. This guard is what protects
   * {@code PartitionExpressionForMetastore} against malicious cross-process (Thrift RPC)
   * payloads. As of this writing Fory's public API exposes no "expected root type" check
   * equivalent to Kryo's; any migration of that call site would need to implement this
   * validation in Hive code (e.g. by checking the deserialized object's class after the fact,
   * which is weaker than Kryo's fail-fast-on-first-class behavior since Fory will have already
   * constructed arbitrary registered/unregistered classes before Hive gets a chance to check).
   */
  @Test
  public void testNoRootTypeGuardEquivalent() {
    MapWork unexpectedType = new MapWork();
    byte[] bytes = fory.serialize(unexpectedType);

    // Unlike SerializationUtilities.deserializeObjectFromKryo(bytes, ExprNodeDesc.class), which
    // throws UnsupportedOperationException when the first class doesn't match the expected root
    // type, Fory's deserialize() has no "expected type" parameter: it fully constructs and
    // returns whatever type was actually serialized. A caller must check the result's class
    // after construction, which is weaker than Kryo's fail-before-constructing behavior.
    Object result = fory.deserialize(bytes);
    Assert.assertTrue(result instanceof MapWork);
  }

  private static MapWork mockMapWork() throws Exception {
    Properties tblProps = new Properties();
    TableDesc tbl = new TableDesc(OrcInputFormat.class, OrcOutputFormat.class, tblProps);

    MapWork mapWork = new MapWork();
    LinkedHashMap<Path, PartitionDesc> partMap = new LinkedHashMap<>();
    for (int p = 0; p < 3; ++p) {
      Path path = new Path("/warehouse/test_table/p=" + p);
      PartitionDesc part = new PartitionDesc(tbl, new LinkedHashMap<>());
      partMap.put(path, part);
    }
    mapWork.setPathToPartitionInfo(partMap);
    return mapWork;
  }
}
