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

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

@UDFType(deterministic = true)
@Description(name = "logged_in_user", value = "_FUNC_() - Returns logged in user name",
        extended = "SessionState GetUserName - the username provided at session initialization")
@NDV(maxNdv = 1)
public class GenericUDFLoggedInUser extends GenericUDF {
  protected Text loggedInUser;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 0) {
      throw new UDFArgumentLengthException(
          "The function LOGGED_IN_USER does not take any arguments, but found " + arguments.length);
    }

    if (loggedInUser == null) {
      String loggedInUserName = SessionState.get().getUserName();
      if (loggedInUserName != null) {
        loggedInUser = new Text(loggedInUserName);
      }
    }

    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    return loggedInUser;
  }

  public Text getLoggedInUser() {
    return loggedInUser;
  }

  public void setLoggedInUser(Text loggedInUser) {
    this.loggedInUser = loggedInUser;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "LOGGED_IN_USER()";
  }

  @Override
  public void copyToNewInstance(Object newInstance) throws UDFArgumentException {
    super.copyToNewInstance(newInstance);
    // Need to preserve loggedInUser
    GenericUDFLoggedInUser other = (GenericUDFLoggedInUser) newInstance;
    if (this.loggedInUser != null) {
      other.loggedInUser = new Text(this.loggedInUser);
    }
  }
}
