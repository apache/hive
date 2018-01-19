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


import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

/**
 * A SettableDecimalObjectInspector can set a HiveDecimal value to an object.
 */
public interface SettableHiveDecimalObjectInspector extends HiveDecimalObjectInspector {

  Object set(Object o, byte[] bytes, int scale);

  Object set(Object o, HiveDecimal t);

  Object set(Object o, HiveDecimalWritable t);

  Object create(byte[] bytes, int scale);

  Object create (HiveDecimal t);

}
