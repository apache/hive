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
package org.apache.hadoop.hive.serde2.lazy;

import org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive.LazyBooleanObjectInspector;
import org.apache.hadoop.io.BooleanWritable;

/**
 * LazyObject for storing a value of boolean.
 * 
 * <p>
 * Part of the code is adapted from Apache Harmony Project.
 * 
 * As with the specification, this implementation relied on code laid out in <a
 * href="http://www.hackersdelight.org/">Henry S. Warren, Jr.'s Hacker's
 * Delight, (Addison Wesley, 2002)</a> as well as <a
 * href="http://aggregate.org/MAGIC/">The Aggregate's Magic Algorithms</a>.
 * </p>
 * 
 */
public class LazyBoolean extends LazyPrimitive<LazyBooleanObjectInspector, BooleanWritable> {

  public LazyBoolean(LazyBooleanObjectInspector oi) {
    super(oi);
    data = new BooleanWritable();
  }

  public LazyBoolean(LazyBoolean copy) {
    super(copy);
    data = new BooleanWritable(copy.data.get());
  }
  
  @Override
  public void init(ByteArrayRef bytes, int start, int length) {
    if (length == 4 
        && Character.toUpperCase(bytes.getData()[start]) == 'T'
        && Character.toUpperCase(bytes.getData()[start+1]) == 'R'
        && Character.toUpperCase(bytes.getData()[start+2]) == 'U'
        && Character.toUpperCase(bytes.getData()[start+3]) == 'E') {
      data.set(true);
      isNull = false;
    } else if (length == 5
          && Character.toUpperCase(bytes.getData()[start]) == 'F'
          && Character.toUpperCase(bytes.getData()[start+1]) == 'A'
          && Character.toUpperCase(bytes.getData()[start+2]) == 'L'
          && Character.toUpperCase(bytes.getData()[start+3]) == 'S'
          && Character.toUpperCase(bytes.getData()[start+4]) == 'E') {
      data.set(false);
      isNull = false;
    } else { 
      isNull = true;
    }
  }
  

}
