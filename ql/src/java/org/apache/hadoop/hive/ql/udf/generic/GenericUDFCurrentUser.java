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
@Description(name = "current_user", value = "_FUNC_() - Returns current user name", extended = "SessionState UserFromAuthenticator")
@NDV(maxNdv = 1)
public class GenericUDFCurrentUser extends GenericUDF {
  protected Text currentUser;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 0) {
      throw new UDFArgumentLengthException(
          "The function CURRENT_USER does not take any arguments, but found " + arguments.length);
    }

    if (currentUser == null) {
      String sessUserFromAuth = SessionState.getUserFromAuthenticator();
      if (sessUserFromAuth != null) {
        currentUser = new Text(sessUserFromAuth);
      }
    }

    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    return currentUser;
  }

  public Text getCurrentUser() {
    return currentUser;
  }

  public void setCurrentUser(Text currentUser) {
    this.currentUser = currentUser;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "CURRENT_USER()";
  }

  @Override
  public void copyToNewInstance(Object newInstance) throws UDFArgumentException {
    super.copyToNewInstance(newInstance);
    // Need to preserve currentUser
    GenericUDFCurrentUser other = (GenericUDFCurrentUser) newInstance;
    if (this.currentUser != null) {
      other.currentUser = new Text(this.currentUser);
    }
  }
}
