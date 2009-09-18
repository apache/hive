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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;


/**
 * A WritableLongObjectInspector inspects a LongWritable Object.
 */
public class WritableLongObjectInspector extends AbstractPrimitiveWritableObjectInspector 
implements SettableLongObjectInspector{

  WritableLongObjectInspector() {
    super(PrimitiveObjectInspectorUtils.longTypeEntry);
  }
  
  @Override
  public long get(Object o) {
    return ((LongWritable)o).get();
  }

  @Override
  public Object copyObject(Object o) {
    return o == null ? null : new LongWritable(((LongWritable)o).get());
  }

  @Override
  public Object getPrimitiveJavaObject(Object o) {
    return o == null ? null : Long.valueOf(((LongWritable)o).get());
  }
  
  @Override
  public Object create(long value) {
    return new LongWritable(value);
  }

  @Override
  public void set(Object o, long value) {
    ((LongWritable)o).set(value);
  }
  
}
