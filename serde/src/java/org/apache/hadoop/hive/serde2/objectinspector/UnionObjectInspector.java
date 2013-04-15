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

package org.apache.hadoop.hive.serde2.objectinspector;

import java.util.List;

/**
 * UnionObjectInspector works on union data that is stored as UnionObject.
 *
 * It holds the list of the object inspectors corresponding to each type of the
 * object the Union can hold.
 *
 * UnionObjectInspector.
 *
 */
public interface UnionObjectInspector extends ObjectInspector {

  /**
   *  Returns the array of ObjectInspectors that are for each of the tags.
   */
  List<ObjectInspector> getObjectInspectors();

  /**
   *   Return the tag of the object.
   */
  byte getTag(Object o);

  /**
   *  Return the field based on the tag associated with the Object.
   */
  Object getField(Object o);

}
