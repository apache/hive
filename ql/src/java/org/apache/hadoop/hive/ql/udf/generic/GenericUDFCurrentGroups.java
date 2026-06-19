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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

/**
 * UDF to get current group from authenticator. This function is not a deterministic function,
 * but a runtime constant. The return value is constant within a query but can be different between queries.
 */
@UDFType(deterministic = false, runtimeConstant = true)
@Description(name = "current_group", value = "_FUNC_() - Returns all groups the current user belongs to",
    extended = "SessionState GroupsFromAuthenticator")
public class GenericUDFCurrentGroups extends GenericUDF {
  protected List<Text> currentGroups;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 0) {
      throw new UDFArgumentLengthException(
          "The function CURRENT_GROUPS does not take any arguments, but found " + arguments.length);
    }

    if (currentGroups == null) {
      List<String> sessGroupsFromAuth = SessionState.getGroupsFromAuthenticator();
      if (sessGroupsFromAuth != null) {
        currentGroups = new ArrayList<Text>();
        for (String group : sessGroupsFromAuth) {
          currentGroups.add(new Text(group));
        }
      }
    }

    return ObjectInspectorFactory.getStandardListObjectInspector(
        PrimitiveObjectInspectorFactory.writableStringObjectInspector);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    return currentGroups;
  }

  public List<Text> getCurrentGroups() {
    return currentGroups;
  }

  public void setCurrentGroups(List<Text> currentGroups) {
    this.currentGroups = currentGroups;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "CURRENT_GROUPS()";
  }

  @Override
  public void copyToNewInstance(Object newInstance) throws UDFArgumentException {
    super.copyToNewInstance(newInstance);
    // Need to preserve currentGroups
    GenericUDFCurrentGroups other = (GenericUDFCurrentGroups) newInstance;
    if (this.currentGroups != null) {
      if (currentGroups != null) {
        other.currentGroups = new ArrayList<Text>();
        for (Text group : currentGroups) {
          other.currentGroups.add(new Text(group));
        }
      }
    }
  }
}
