/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.VectorPartitionDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Assert;
import org.junit.Test;

public class TestSerializationUtilities {

  @Test
  public void testEveryPropertiesAreSerialized() throws Exception {
    MapWork mapWork = doSerDeser(null);

    assertPartitionDescPropertyPresence(mapWork, "/warehouse/test_table/p=0", "impala_intermediate_stats_chunk1", true);
    assertPartitionDescPropertyPresence(mapWork, "/warehouse/test_table/p=0", "impala_intermediate_stats_chunk2", true);
    assertPartitionDescPropertyPresence(mapWork, "/warehouse/test_table/p=0", "rawDataSize", true);
    assertPartitionDescPropertyPresence(mapWork, "/warehouse/test_table/p=0", "serialization.ddl", true);
  }

  @Test
  public void testRegexFilterAll() throws Exception {
    MapWork mapWork = doSerDeser(getConfWithSkipConfig(".*"));

    assertPartitionDescPropertyPresence(mapWork, "/warehouse/test_table/p=0", "impala_intermediate_stats_chunk1",
        false);
    assertPartitionDescPropertyPresence(mapWork, "/warehouse/test_table/p=0", "impala_intermediate_stats_chunk2",
        false);
    assertPartitionDescPropertyPresence(mapWork, "/warehouse/test_table/p=0", "rawDataSize", false);
    assertPartitionDescPropertyPresence(mapWork, "/warehouse/test_table/p=0", "serialization.ddl", false);
  }

  @Test
  public void testRegexFilterSomeProps() throws Exception {
    MapWork mapWork = doSerDeser(getConfWithSkipConfig("impala_intermediate_stats_chunk.*"));

    assertPartitionDescPropertyPresence(mapWork, "/warehouse/test_table/p=0", "impala_intermediate_stats_chunk1",
        false);
    assertPartitionDescPropertyPresence(mapWork, "/warehouse/test_table/p=0", "impala_intermediate_stats_chunk2",
        false);
    assertPartitionDescPropertyPresence(mapWork, "/warehouse/test_table/p=0", "rawDataSize", true);
    assertPartitionDescPropertyPresence(mapWork, "/warehouse/test_table/p=0", "serialization.ddl", true);
  }

  @Test
  public void testString() throws Exception {
    MapWork mapWork = doSerDeser(getConfWithSkipConfig("rawDataSize"));

    assertPartitionDescPropertyPresence(mapWork, "/warehouse/test_table/p=0", "impala_intermediate_stats_chunk1", true);
    assertPartitionDescPropertyPresence(mapWork, "/warehouse/test_table/p=0", "impala_intermediate_stats_chunk2", true);
    assertPartitionDescPropertyPresence(mapWork, "/warehouse/test_table/p=0", "rawDataSize", false);
    assertPartitionDescPropertyPresence(mapWork, "/warehouse/test_table/p=0", "serialization.ddl", true);
  }

  @Test
  public void testStringAndBadRegex() throws Exception {
    MapWork mapWork = doSerDeser(getConfWithSkipConfig("impala_intermediate_stats_chunk,rawDataSize"));

    // impala_intermediate_stats_chunk props are not filtered in this case because only "*" activates regex mode
    assertPartitionDescPropertyPresence(mapWork, "/warehouse/test_table/p=0", "impala_intermediate_stats_chunk1", true);
    assertPartitionDescPropertyPresence(mapWork, "/warehouse/test_table/p=0", "impala_intermediate_stats_chunk2", true);
    assertPartitionDescPropertyPresence(mapWork, "/warehouse/test_table/p=0", "rawDataSize", false);
    assertPartitionDescPropertyPresence(mapWork, "/warehouse/test_table/p=0", "serialization.ddl", true);
  }

  @Test
  public void testStringRegexMixed() throws Exception {
    MapWork mapWork = doSerDeser(getConfWithSkipConfig("impala_intermediate_stats_chunk.*,rawDataSize,.*ddl"));

    assertPartitionDescPropertyPresence(mapWork, "/warehouse/test_table/p=0", "impala_intermediate_stats_chunk1",
        false);
    assertPartitionDescPropertyPresence(mapWork, "/warehouse/test_table/p=0", "impala_intermediate_stats_chunk2",
        false);
    assertPartitionDescPropertyPresence(mapWork, "/warehouse/test_table/p=0", "rawDataSize", false);
    assertPartitionDescPropertyPresence(mapWork, "/warehouse/test_table/p=0", "serialization.ddl", false);
  }

  @Test
  public void testSkippingAppliesToAllPartitions() throws Exception {
    MapWork mapWork = doSerDeser(getConfWithSkipConfig("impala_intermediate_stats_chunk.*,rawDataSize,.*ddl"));

    assertPartitionDescPropertyPresence(mapWork, "/warehouse/test_table/p=0", "impala_intermediate_stats_chunk1",
        false);
    assertPartitionDescPropertyPresence(mapWork, "/warehouse/test_table/p=0", "impala_intermediate_stats_chunk2",
        false);
    assertPartitionDescPropertyPresence(mapWork, "/warehouse/test_table/p=0", "rawDataSize", false);
    assertPartitionDescPropertyPresence(mapWork, "/warehouse/test_table/p=0", "serialization.ddl", false);

    assertPartitionDescPropertyPresence(mapWork, "/warehouse/test_table/p=1", "impala_intermediate_stats_chunk1",
        false);
    assertPartitionDescPropertyPresence(mapWork, "/warehouse/test_table/p=1", "impala_intermediate_stats_chunk2",
        false);
    assertPartitionDescPropertyPresence(mapWork, "/warehouse/test_table/p=1", "rawDataSize", false);
    assertPartitionDescPropertyPresence(mapWork, "/warehouse/test_table/p=1", "serialization.ddl", false);
  }

  @Test
  public void testUnsupportedDeserialization() throws Exception {
    ArrayList<Long> invalidExpr = new ArrayList<>();
    invalidExpr.add(1L);
    byte[] buf = SerializationUtilities.serializeObjectWithTypeInformation(invalidExpr);
    try {
      SerializationUtilities.deserializeObjectWithTypeInformation(buf, true);
      Assert.fail("Should throw exception as the input is not a valid filter");
    } catch (UnsupportedOperationException e) {
      // ignore
    }

    ExprNodeDesc validExpr = ExprNodeGenericFuncDesc.newInstance(new GenericUDFOPNull(),
        Arrays.asList(new ExprNodeColumnDesc(new ColumnInfo("_c0", TypeInfoFactory.stringTypeInfo, "a", false))));
    buf = SerializationUtilities.serializeObjectWithTypeInformation(validExpr);
    ExprNodeDesc desc = SerializationUtilities.deserializeObjectWithTypeInformation(buf, true);
    Assert.assertTrue(ExprNodeDescUtils.isSame(validExpr, desc));
  }

  private MapWork doSerDeser(Configuration configuration) throws Exception, IOException {
    MapWork mapWork = mockMapWorkWithSomePartitionDescProperties();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    SerializationUtilities.serializePlan(mapWork, baos, configuration);
    InputStream bais = new ByteArrayInputStream(baos.toByteArray());
    MapWork mapWorkDeserialized = SerializationUtilities.deserializePlan(bais, MapWork.class);
    baos.close();
    bais.close();
    return mapWorkDeserialized;
  }

  private Configuration getConfWithSkipConfig(String value) {
    Configuration configuration = new Configuration();
    HiveConf.setVar(configuration, HiveConf.ConfVars.HIVE_PLAN_MAPWORK_SERIALIZATION_SKIP_PROPERTIES, value);
    return configuration;
  }

  private void assertPartitionDescPropertyPresence(MapWork mapWork, String partitionPath, String prop,
      boolean isPresent) {
    String value = mapWork.getPathToPartitionInfo().get(new Path(partitionPath)).getProperties().getProperty(prop);
    Assert.assertTrue(String.format("'%s' is%ssupposed to be present", prop, (isPresent ? " " : " not ")),
        isPresent ? value != null : value == null);
  }

  private static MapWork mockMapWorkWithSomePartitionDescProperties() throws Exception {
    String tableName = "test_table";
    int numPartitions = 2;
    Path root = new Path("/warehouse", "test_table");

    String[] partPath = new String[numPartitions];
    StringBuilder buffer = new StringBuilder();
    for (int p = 0; p < numPartitions; ++p) {
      partPath[p] = new Path(root, "p=" + p).toString();
      if (p != 0) {
        buffer.append(',');
      }
      buffer.append(partPath[p]);
    }

    Properties tblProps = new Properties();
    TableDesc tbl = new TableDesc(OrcInputFormat.class, OrcOutputFormat.class, tblProps);

    MapWork mapWork = new MapWork();

    Map<Path, List<String>> aliasMap = new LinkedHashMap<>();
    List<String> aliases = new ArrayList<String>();
    aliases.add(tableName);

    LinkedHashMap<Path, PartitionDesc> partMap = new LinkedHashMap<>();
    for (int p = 0; p < numPartitions; ++p) {
      Path path = new Path(partPath[p]);
      aliasMap.put(path, aliases);
      LinkedHashMap<String, String> partSpec = new LinkedHashMap<String, String>();
      PartitionDesc part = new PartitionDesc(tbl, partSpec);
      part.setVectorPartitionDesc(
          VectorPartitionDesc.createVectorizedInputFileFormat("MockInputFileFormatClassName", false, null));

      part.getProperties().put("impala_intermediate_stats_chunk1", "asdfghjk12345678");
      part.getProperties().put("impala_intermediate_stats_chunk2", "asdfghjk12345678");
      part.getProperties().put("rawDataSize", "10");
      part.getProperties().put("serialization.ddl", "asdf");

      partMap.put(path, part);
    }
    mapWork.setPathToAliases(aliasMap);
    mapWork.setPathToPartitionInfo(partMap);

    return mapWork;
  }
}
