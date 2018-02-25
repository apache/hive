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
package org.apache.hadoop.hive.serde2.objectinspector.primitive;

import java.sql.Date;

import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * A JavaDateObjectInspector inspects a Java Date Object.
 */
public class JavaDateObjectInspector
    extends AbstractPrimitiveJavaObjectInspector
    implements SettableDateObjectInspector {

  protected JavaDateObjectInspector() {
    super(TypeInfoFactory.dateTypeInfo);
  }

  public DateWritable getPrimitiveWritableObject(Object o) {
    return o == null ? null : new DateWritable((Date) o);
  }

  @Override
  public Date getPrimitiveJavaObject(Object o) {
    return o == null ? null : (Date) o;
  }

  public Date get(Object o) {
    return (Date) o;
  }

  public Object set(Object o, Date value) {
    if (value == null) {
      return null;
    }
    ((Date) o).setTime(value.getTime());
    return o;
  }

  public Object set(Object o, DateWritable d) {
    if (d == null) {
      return null;
    }
    ((Date) o).setTime(d.get().getTime());
    return o;
  }

  public Object create(Date value) {
    return new Date(value.getTime());
  }

}
