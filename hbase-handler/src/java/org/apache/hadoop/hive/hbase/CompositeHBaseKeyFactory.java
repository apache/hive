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

package org.apache.hadoop.hive.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.lazy.objectinspector.LazySimpleStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class CompositeHBaseKeyFactory<T extends HBaseCompositeKey>
    extends DefaultHBaseKeyFactory implements Configurable {

  public static final Log LOG = LogFactory.getLog(CompositeHBaseKeyFactory.class);

  private final Class<T> keyClass;
  private final Constructor constructor;

  private Configuration conf;

  public CompositeHBaseKeyFactory(Class<T> keyClass) throws Exception {
    // see javadoc of HBaseCompositeKey
    this.keyClass = keyClass;
    this.constructor = keyClass.getDeclaredConstructor(
        LazySimpleStructObjectInspector.class, Properties.class, Configuration.class);
  }

  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) throws IOException {
    super.configureJobConf(tableDesc, jobConf);
    TableMapReduceUtil.addDependencyJars(jobConf, keyClass);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public T createKey(ObjectInspector inspector) throws SerDeException {
    try {
      return (T) constructor.newInstance(inspector, properties, conf);
    } catch (Exception e) {
      throw new SerDeException(e);
    }
  }

  @Override
  public DecomposedPredicate decomposePredicate(JobConf jobConf, Deserializer deserializer,
      ExprNodeDesc predicate) {
    String keyColName = hbaseParams.getKeyColumnMapping().columnName;

    IndexPredicateAnalyzer analyzer = IndexPredicateAnalyzer.createAnalyzer(true);
    analyzer.allowColumnName(keyColName);
    analyzer.setAcceptsFields(true);
    analyzer.setFieldValidator(new Validator());

    DecomposedPredicate decomposed = new DecomposedPredicate();

    List<IndexSearchCondition> conditions = new ArrayList<IndexSearchCondition>();
    decomposed.residualPredicate =
        (ExprNodeGenericFuncDesc)analyzer.analyzePredicate(predicate, conditions);
    if (!conditions.isEmpty()) {
      decomposed.pushedPredicate = analyzer.translateSearchConditions(conditions);
      try {
        decomposed.pushedPredicateObject = setupFilter(keyColName, conditions);
      } catch (Exception e) {
        LOG.warn("Failed to decompose predicates", e);
        return null;
      }
    }
    return decomposed;
  }

  protected Serializable setupFilter(String keyColName, List<IndexSearchCondition> conditions)
      throws Exception {
    HBaseScanRange scanRange = new HBaseScanRange();
    for (IndexSearchCondition condition : conditions) {
      if (condition.getFields() == null) {
        continue;
      }
      String field = condition.getFields()[0];
      Object value = condition.getConstantDesc().getValue();
      scanRange.addFilter(new FamilyFilter(
          CompareFilter.CompareOp.EQUAL, new BinaryComparator(field.getBytes())));
    }
    return scanRange;
  }

  private static class Validator implements IndexPredicateAnalyzer.FieldValidator {

    /**
     * Validates the field in the {@link ExprNodeFieldDesc}. Basically this validates that the given field is the first field in the given struct.
     * This is important specially in case of structs as order of fields in the structs is important when using for any filter down the line
     **/
    public boolean validate(ExprNodeFieldDesc fieldDesc) {
      String fieldName = fieldDesc.getFieldName();

      ExprNodeDesc nodeDesc = fieldDesc.getDesc();

      TypeInfo typeInfo = nodeDesc.getTypeInfo();

      if (!(typeInfo instanceof StructTypeInfo)) {
        // since we are working off a ExprNodeFieldDesc which represents a field within a struct, this
        // should never happen
        throw new AssertionError("Expected StructTypeInfo. Found:" + typeInfo.getTypeName());
      }

      List<String> allFieldNames = ((StructTypeInfo) typeInfo).getAllStructFieldNames();

      if (allFieldNames == null || allFieldNames.size() == 0) {
        return false;
      }

      String firstElement = allFieldNames.get(0);

      return firstElement.equals(fieldName);
    }
  }
}