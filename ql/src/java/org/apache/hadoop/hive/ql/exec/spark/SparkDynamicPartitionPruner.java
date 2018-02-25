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

package org.apache.hadoop.hive.ql.exec.spark;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.clearspring.analytics.util.Preconditions;
import javolution.testing.AssertionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * The spark version of DynamicPartitionPruner.
 */
public class SparkDynamicPartitionPruner {
  private static final Logger LOG = LoggerFactory.getLogger(SparkDynamicPartitionPruner.class);
  private final Map<String, List<SourceInfo>> sourceInfoMap = new LinkedHashMap<String, List<SourceInfo>>();
  private final BytesWritable writable = new BytesWritable();

  public void prune(MapWork work, JobConf jobConf) throws HiveException, SerDeException {
    sourceInfoMap.clear();
    initialize(work, jobConf);
    if (sourceInfoMap.size() == 0) {
      // Nothing to prune for this MapWork
      return;
    }
    processFiles(work, jobConf);
    prunePartitions(work);
  }

  public void initialize(MapWork work, JobConf jobConf) throws SerDeException {
    Map<String, SourceInfo> columnMap = new HashMap<String, SourceInfo>();
    Set<String> sourceWorkIds = work.getEventSourceTableDescMap().keySet();

    for (String id : sourceWorkIds) {
      List<TableDesc> tables = work.getEventSourceTableDescMap().get(id);
       // Real column name - on which the operation is being performed
      List<String> columnNames = work.getEventSourceColumnNameMap().get(id);
      // Column type
      List<String> columnTypes = work.getEventSourceColumnTypeMap().get(id);
      List<ExprNodeDesc> partKeyExprs = work.getEventSourcePartKeyExprMap().get(id);

      Iterator<String> cit = columnNames.iterator();
      Iterator<String> typit = columnTypes.iterator();
      Iterator<ExprNodeDesc> pit = partKeyExprs.iterator();
      for (TableDesc t : tables) {
        String columnName = cit.next();
        String columnType = typit.next();
        ExprNodeDesc partKeyExpr = pit.next();
        SourceInfo si = new SourceInfo(t, partKeyExpr, columnName, columnType, jobConf);
        if (!sourceInfoMap.containsKey(id)) {
          sourceInfoMap.put(id, new ArrayList<SourceInfo>());
        }
        sourceInfoMap.get(id).add(si);

        // We could have multiple sources restrict the same column, need to take
        // the union of the values in that case.
        if (columnMap.containsKey(columnName)) {
          si.values = columnMap.get(columnName).values;
        }
        columnMap.put(columnName, si);
      }
    }
  }

  private void processFiles(MapWork work, JobConf jobConf) throws HiveException {
    ObjectInputStream in = null;
    try {
      Path baseDir = work.getTmpPathForPartitionPruning();
      FileSystem fs = FileSystem.get(baseDir.toUri(), jobConf);

      // Find the SourceInfo to put values in.
      for (String name : sourceInfoMap.keySet()) {
        Path sourceDir = new Path(baseDir, name);
        for (FileStatus fstatus : fs.listStatus(sourceDir)) {
          LOG.info("Start processing pruning file: " + fstatus.getPath());
          in = new ObjectInputStream(fs.open(fstatus.getPath()));
          final int numName = in.readInt();
          SourceInfo info = null;

          Set<String> columnNames = new HashSet<>();
          for (int i = 0; i < numName; i++) {
            columnNames.add(in.readUTF());
          }
          for (SourceInfo si : sourceInfoMap.get(name)) {
            if (columnNames.contains(si.columnName)) {
              info = si;
              break;
            }
          }

          Preconditions.checkArgument(info != null,
              "AssertionError: no source info for the column: " +
                  Arrays.toString(columnNames.toArray()));

          // Read fields
          while (in.available() > 0) {
            writable.readFields(in);

            Object row = info.deserializer.deserialize(writable);
            Object value = info.soi.getStructFieldData(row, info.field);
            value = ObjectInspectorUtils.copyToStandardObject(value, info.fieldInspector);
            info.values.add(value);
          }
        }
      }
    } catch (Exception e) {
      throw new HiveException(e);
    } finally {
      try {
        if (in != null) {
          in.close();
        }
      } catch (IOException e) {
        throw new HiveException("error while trying to close input stream", e);
      }
    }
  }

  private void prunePartitions(MapWork work) throws HiveException {
    for (String source : sourceInfoMap.keySet()) {
      for (SourceInfo info : sourceInfoMap.get(source)) {
        prunePartitionSingleSource(info, work);
      }
    }
  }

  private void prunePartitionSingleSource(SourceInfo info, MapWork work)
      throws HiveException {
    Set<Object> values = info.values;
    // strip the column name of the targetId
    String columnName = info.columnName.substring(info.columnName.indexOf(':') + 1);

    ObjectInspector oi =
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory
            .getPrimitiveTypeInfo(info.columnType));

    ObjectInspectorConverters.Converter converter =
        ObjectInspectorConverters.getConverter(
            PrimitiveObjectInspectorFactory.javaStringObjectInspector, oi);

    StructObjectInspector soi =
        ObjectInspectorFactory.getStandardStructObjectInspector(
            Collections.singletonList(columnName), Collections.singletonList(oi));

    @SuppressWarnings("rawtypes")
    ExprNodeEvaluator eval = ExprNodeEvaluatorFactory.get(info.partKey);
    eval.initialize(soi);

    applyFilterToPartitions(work, converter, eval, columnName, values);
  }

  private void applyFilterToPartitions(
      MapWork work,
      ObjectInspectorConverters.Converter converter,
      ExprNodeEvaluator eval,
      String columnName,
      Set<Object> values) throws HiveException {

    Object[] row = new Object[1];

    Iterator<Path> it = work.getPathToPartitionInfo().keySet().iterator();
    while (it.hasNext()) {
      Path p = it.next();
      PartitionDesc desc = work.getPathToPartitionInfo().get(p);
      Map<String, String> spec = desc.getPartSpec();
      if (spec == null) {
        throw new AssertionException("No partition spec found in dynamic pruning");
      }

      String partValueString = spec.get(columnName);
      if (partValueString == null) {
        throw new AssertionException("Could not find partition value for column: " + columnName);
      }

      Object partValue = converter.convert(partValueString);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Converted partition value: " + partValue + " original (" + partValueString + ")");
      }

      row[0] = partValue;
      partValue = eval.evaluate(row);
      if (LOG.isDebugEnabled()) {
        LOG.debug("part key expr applied: " + partValue);
      }

      if (!values.contains(partValue)) {
        LOG.info("Pruning path: " + p);
        it.remove();
        work.removePathToAlias(p);
        // HIVE-12244 call currently ineffective
        work.getPartitionDescs().remove(desc);
      }
    }
  }

  @SuppressWarnings("deprecation")
  private static class SourceInfo {
    final ExprNodeDesc partKey;
    final Deserializer deserializer;
    final StructObjectInspector soi;
    final StructField field;
    final ObjectInspector fieldInspector;
    Set<Object> values = new HashSet<Object>();
    final String columnName;
    final String columnType;

    SourceInfo(TableDesc table, ExprNodeDesc partKey, String columnName, String columnType, JobConf jobConf)
        throws SerDeException {
      this.partKey = partKey;
      this.columnName = columnName;
      this.columnType = columnType;

      deserializer = ReflectionUtils.newInstance(table.getDeserializerClass(), null);
      deserializer.initialize(jobConf, table.getProperties());

      ObjectInspector inspector = deserializer.getObjectInspector();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Type of obj insp: " + inspector.getTypeName());
      }

      soi = (StructObjectInspector) inspector;
      List<? extends StructField> fields = soi.getAllStructFieldRefs();
      assert(fields.size() > 1) : "expecting single field in input";

      field = fields.get(0);
      fieldInspector =
          ObjectInspectorUtils.getStandardObjectInspector(field.getFieldObjectInspector());
    }
  }

}
