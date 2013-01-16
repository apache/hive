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
package org.apache.hadoop.hive.serde2.objectinspector.primitive;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.hadoop.hive.serde2.io.BigDecimalWritable;

public class JavaBigDecimalObjectInspector
    extends AbstractPrimitiveJavaObjectInspector
    implements SettableBigDecimalObjectInspector {

  protected JavaBigDecimalObjectInspector() {
    super(PrimitiveObjectInspectorUtils.decimalTypeEntry);
  }

  @Override
  public BigDecimalWritable getPrimitiveWritableObject(Object o) {
    return o == null ? null : new BigDecimalWritable((BigDecimal) o);
  }

  @Override
  public BigDecimal getPrimitiveJavaObject(Object o) {
    return o == null ? null : (BigDecimal) o;
  }

  @Override
  public Object set(Object o, byte[] bytes, int scale) {
    return new BigDecimal(new BigInteger(bytes), scale);
  }

  @Override
  public Object set(Object o, BigDecimal t) {
    return t;
  }

  @Override
  public Object set(Object o, BigDecimalWritable t) {
    return t == null ? null : t.getBigDecimal();
  }

  @Override
  public Object create(byte[] bytes, int scale) {
    return new BigDecimal(new BigInteger(bytes), scale);
  }

  @Override
  public Object create(BigDecimal t) {
    return t == null ? null : new BigDecimal(t.unscaledValue(), t.scale());
  }

}
