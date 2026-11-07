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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.LinkedHashMap;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.CopyOnFirstWriteProperties;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.junit.Assert;
import org.junit.Test;

/** Round trips a MapWork through both plan serialization engines selected by hive.plan.serialization.engine. */
public class TestPlanSerializationEngines {

  private static HiveConf confFor(String engine) {
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.HIVE_PLAN_SERIALIZATION_ENGINE, engine);
    return conf;
  }

  private static MapWork mapWork() throws Exception {
    Properties tblProps = new Properties();
    tblProps.setProperty("columns", "a,b");
    tblProps.setProperty("rawDataSize", "42");
    TableDesc tbl = new TableDesc(OrcInputFormat.class, OrcOutputFormat.class, tblProps);
    MapWork mapWork = new MapWork();
    LinkedHashMap<Path, PartitionDesc> parts = new LinkedHashMap<>();
    for (int i = 0; i < 5; i++) {
      PartitionDesc part = new PartitionDesc(tbl, new LinkedHashMap<>());
      part.setProperties(new CopyOnFirstWriteProperties(tblProps));
      parts.put(new Path("/warehouse/t/p=" + i), part);
    }
    mapWork.setPathToPartitionInfo(parts);
    return mapWork;
  }

  private static MapWork roundTrip(String engine, MapWork in) {
    HiveConf conf = confFor(engine);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    SerializationUtilities.serializePlanWithEngine(conf, in, out);
    return SerializationUtilities.deserializePlanWithEngine(conf, new ByteArrayInputStream(out.toByteArray()),
        MapWork.class);
  }

  @Test
  public void testRoundTripBothEngines() throws Exception {
    for (String engine : new String[] {"kryo", "fory"}) {
      MapWork in = mapWork();
      MapWork result = roundTrip(engine, in);
      Assert.assertEquals(engine, in.getPathToPartitionInfo().keySet(), result.getPathToPartitionInfo().keySet());
      for (PartitionDesc part : result.getPathToPartitionInfo().values()) {
        Assert.assertEquals(engine, "a,b", part.getProperties().getProperty("columns"));
        Assert.assertEquals(engine, "42", part.getProperties().getProperty("rawDataSize"));
      }
    }
  }

  @Test
  public void testForyPayloadHasMagicAndKryoPayloadDoesNot() throws Exception {
    ByteArrayOutputStream fory = new ByteArrayOutputStream();
    SerializationUtilities.serializePlanWithEngine(confFor("fory"), mapWork(), fory);
    Assert.assertEquals("FORY", new String(fory.toByteArray(), 0, 4, java.nio.charset.StandardCharsets.US_ASCII));

    ByteArrayOutputStream kryo = new ByteArrayOutputStream();
    SerializationUtilities.serializePlanWithEngine(confFor("kryo"), mapWork(), kryo);
    Assert.assertNotEquals("FORY", new String(kryo.toByteArray(), 0, 4, java.nio.charset.StandardCharsets.US_ASCII));
  }

  @Test
  public void testReaderDetectsEngineFromDataNotConf() throws Exception {
    ByteArrayOutputStream kryo = new ByteArrayOutputStream();
    SerializationUtilities.serializePlanWithEngine(confFor("kryo"), mapWork(), kryo);
    MapWork fromKryoBytes = SerializationUtilities.deserializePlanWithEngine(confFor("fory"),
        new ByteArrayInputStream(kryo.toByteArray()), MapWork.class);
    Assert.assertEquals(5, fromKryoBytes.getPathToPartitionInfo().size());

    ByteArrayOutputStream fory = new ByteArrayOutputStream();
    SerializationUtilities.serializePlanWithEngine(confFor("fory"), mapWork(), fory);
    MapWork fromForyBytes = SerializationUtilities.deserializePlanWithEngine(confFor("kryo"),
        new ByteArrayInputStream(fory.toByteArray()), MapWork.class);
    Assert.assertEquals(5, fromForyBytes.getPathToPartitionInfo().size());
  }

  @Test
  public void testSkipPropertiesFilterAppliesToBothEngines() throws Exception {
    for (String engine : new String[] {"kryo", "fory"}) {
      HiveConf conf = confFor(engine);
      conf.setVar(HiveConf.ConfVars.HIVE_PLAN_MAPWORK_SERIALIZATION_SKIP_PROPERTIES, "rawDataSize");
      // Distinct Properties per partition: the filter removes keys from the partition's own properties.
      MapWork in = mapWork();
      for (PartitionDesc part : in.getPathToPartitionInfo().values()) {
        Properties own = new Properties();
        own.setProperty("columns", "a,b");
        own.setProperty("rawDataSize", "42");
        part.setProperties(own);
      }
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      SerializationUtilities.serializePlanWithEngine(conf, in, out);
      MapWork result = SerializationUtilities.deserializePlanWithEngine(conf,
          new ByteArrayInputStream(out.toByteArray()), MapWork.class);
      for (PartitionDesc part : result.getPathToPartitionInfo().values()) {
        Assert.assertNull(engine, part.getProperties().getProperty("rawDataSize"));
        Assert.assertEquals(engine, "a,b", part.getProperties().getProperty("columns"));
      }
    }
  }
}
