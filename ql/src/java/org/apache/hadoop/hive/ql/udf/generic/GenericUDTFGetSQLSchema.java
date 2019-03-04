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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * GenericUDTFGetSQLSchema.
 */
@Description(name = "get_sql_schema", value = "_FUNC_(string) - "
    + "Takes query as argument. Returns schema (column names and types) of the resultset " +
    " that would be generated when the query is executed. " +
    "Can be invoked like: select get_sql_schema(\"select * from some_table\")." +
    "NOTE: This does not produce any output for DDL queries like show tables/databases/... and others.")
@UDFType(deterministic = false)
public class GenericUDTFGetSQLSchema extends GenericUDTF {
  private static final Logger LOG = LoggerFactory.getLogger(GenericUDTFGetSQLSchema.class);

  protected transient StringObjectInspector stringOI;
  protected transient JobConf jc;

  private final transient Object[] nameTypePair = new Object[2];

  @Override
  public void process(Object[] arguments) throws HiveException {

    String query = stringOI.getPrimitiveJavaObject(arguments[0]);
    LOG.debug("Getting schema for Query: {}", query);
    HiveConf conf = new HiveConf(SessionState.get().getConf());
    List<FieldSchema> fieldSchemas = null;
    try {
      fieldSchemas = ParseUtils.parseQueryAndGetSchema(conf, query);
    } catch (IOException | ParseException e) {
      throw new HiveException(e);
    }

    if (fieldSchemas != null) {
      for (FieldSchema fieldSchema : fieldSchemas) {
        nameTypePair[0] = fieldSchema.getName().getBytes(StandardCharsets.UTF_8);
        nameTypePair[1] = fieldSchema.getType().getBytes(StandardCharsets.UTF_8);
        forward(nameTypePair);
      }
    }
  }

  @Override
  public StructObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {

    LOG.debug("initializing GenericUDTFGetSQLSchema");

    if (SessionState.get() == null || SessionState.get().getConf() == null) {
      throw new IllegalStateException("Cannot run GET_SQL_SCHEMA outside HS2");
    }
    LOG.debug("Initialized conf, jc and metastore connection");

    if (arguments.length != 1) {
      throw new UDFArgumentLengthException(
          "The function GET_SQL_SCHEMA accepts 1 argument.");
    } else if (!(arguments[0] instanceof StringObjectInspector)) {
      LOG.error("Got " + arguments[0].getTypeName() + " instead of string.");
      throw new UDFArgumentTypeException(0, "\""
          + "string\" is expected at function GET_SQL_SCHEMA, " + "but \""
          + arguments[0].getTypeName() + "\" is found");
    }

    stringOI = (StringObjectInspector) arguments[0];

    List<String> names = Arrays.asList("col_name", "col_type");
    List<ObjectInspector> fieldOIs = Arrays.asList(PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector,
        PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector);
    StructObjectInspector outputOI = ObjectInspectorFactory
        .getStandardStructObjectInspector(names, fieldOIs);

    LOG.debug("done initializing GenericUDTFGetSQLSchema");
    return outputOI;
  }

  @Override
  public String toString() {
    return "get_sql_schema";
  }

  @Override
  public void close() throws HiveException {
  }
}
