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

package org.apache.hadoop.hive.ql.udf.generic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringUpper;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.stats.estimator.StatEstimator;
import org.apache.hadoop.hive.ql.stats.estimator.StatEstimatorProvider;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.util.List;
import java.util.Optional;

/**
 * UDFXmlTest.
 */
@Description(name = "xmltest",
  value = "_FUNC_() - Returns XML string",
  extended = "Example:\n"
    + "  > SELECT _FUNC_() FROM src LIMIT 1;\n" + "  '<Tag><element>test</element></Tag>'")
@VectorizedExpressions({StringUpper.class})
public class GenericUDFXmlTest extends GenericUDF implements StatEstimatorProvider {

//  private static final XmlMapper xmlMapper = new XmlMapper();

  private final Text text = new Text();
  private final Tag tag = new Tag("test");

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
//    try {
//      text.set(xmlMapper.writeValueAsString(tag));
//    } catch (JsonProcessingException e) {
//      throw new RuntimeException(e);
//    }
    return text;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("xmltest", children);
  }

  @Override
  public StatEstimator getStatEstimator() {
    return new StatEstimator() {
      @Override
      public Optional<ColStatistics> estimate(List<ColStatistics> argStats) {
        return Optional.of(argStats.get(0).clone());
      }
    };
  }

}

class Tag {
  public Tag(final String tag) {
    this.element = tag;
  }

  public String element;
}