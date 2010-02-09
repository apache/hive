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
package org.apache.hadoop.hive.contrib.genericudf.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.IntWritable;

/**
 * GenericUDFDBOutput is designed to output data directly from Hive to a JDBC
 * datastore. This UDF is useful for exporting small to medium summaries that
 * have a unique key.
 * 
 * Due to the nature of hadoop, individual mappers, reducers or entire jobs can
 * fail. If a failure occurs a mapper or reducer may be retried. This UDF has no
 * way of detecting failures or rolling back a transaction. Consequently, you
 * should only only use this to export to a table with a unique key. The unique
 * key should safeguard against duplicate data.
 * 
 * Use hive's ADD JAR feature to add your JDBC Driver to the distributed cache,
 * otherwise GenericUDFDBoutput will fail.
 */
@Description(name = "dboutput",
    value = "_FUNC_(jdbcstring,username,password,preparedstatement,[arguments])"
    + " - sends data to a jdbc driver",
    extended = "argument 0 is the JDBC connection string\n"
    + "argument 1 is the user name\n"
    + "argument 2 is the password\n"
    + "argument 3 is an SQL query to be used in the PreparedStatement\n"
    + "argument (4-n) The remaining arguments must be primitive and are "
    + "passed to the PreparedStatement object\n")
@UDFType(deterministic = false)
public class GenericUDFDBOutput extends GenericUDF {
  private static Log LOG = LogFactory
      .getLog(GenericUDFDBOutput.class.getName());

  ObjectInspector[] argumentOI;
  GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;
  Connection connection = null;
  private String url;
  private String user;
  private String pass;
  private final IntWritable result = new IntWritable(-1);

  /**
   * @param arguments
   *          argument 0 is the JDBC connection string argument 1 is the user
   *          name argument 2 is the password argument 3 is an SQL query to be
   *          used in the PreparedStatement argument (4-n) The remaining
   *          arguments must be primitive and are passed to the
   *          PreparedStatement object
   */
  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentTypeException {
    argumentOI = arguments;

    // this should be connection url,username,password,query,column1[,columnn]*
    for (int i = 0; i < 4; i++) {
      if (arguments[i].getCategory() == ObjectInspector.Category.PRIMITIVE) {
        PrimitiveObjectInspector poi = ((PrimitiveObjectInspector) arguments[i]);

        if (!(poi.getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING)) {
          throw new UDFArgumentTypeException(i,
              "The argument of function  should be \""
              + Constants.STRING_TYPE_NAME + "\", but \""
              + arguments[i].getTypeName() + "\" is found");
        }
      }
    }
    for (int i = 4; i < arguments.length; i++) {
      if (arguments[i].getCategory() != ObjectInspector.Category.PRIMITIVE) {
        throw new UDFArgumentTypeException(i,
            "The argument of function should be primative" + ", but \""
            + arguments[i].getTypeName() + "\" is found");
      }
    }

    return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
  }

  /**
   * @return 0 on success -1 on failure
   */
  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {

    url = ((StringObjectInspector) argumentOI[0])
        .getPrimitiveJavaObject(arguments[0].get());
    user = ((StringObjectInspector) argumentOI[1])
        .getPrimitiveJavaObject(arguments[1].get());
    pass = ((StringObjectInspector) argumentOI[2])
        .getPrimitiveJavaObject(arguments[2].get());

    try {
      connection = DriverManager.getConnection(url, user, pass);
    } catch (SQLException ex) {
      LOG.error("Driver loading or connection issue", ex);
      result.set(2);
    }

    if (connection != null) {
      try {

        PreparedStatement ps = connection
            .prepareStatement(((StringObjectInspector) argumentOI[3])
            .getPrimitiveJavaObject(arguments[3].get()));
        for (int i = 4; i < arguments.length; ++i) {
          PrimitiveObjectInspector poi = ((PrimitiveObjectInspector) argumentOI[i]);
          ps.setObject(i - 3, poi.getPrimitiveJavaObject(arguments[i].get()));
        }
        ps.execute();
        ps.close();
        result.set(0);
      } catch (SQLException e) {
        LOG.error("Underlying SQL exception", e);
        result.set(1);
      } finally {
        try {
          connection.close();
        } catch (Exception ex) {
          LOG.error("Underlying SQL exception during close", ex);
        }
      }
    }

    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    StringBuilder sb = new StringBuilder();
    sb.append("dboutput(");
    if (children.length > 0) {
      sb.append(children[0]);
      for (int i = 1; i < children.length; i++) {
        sb.append(",");
        sb.append(children[i]);
      }
    }
    sb.append(")");
    return sb.toString();
  }

}
