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

package org.apache.hadoop.hive.ql.metadata;

import java.util.Random;

/**
 * A random dimension is an abstract dimension. It is implicitly associated with
 * every row in data and has a random value
 * 
 **/
public class RandomDimension extends Dimension {

  Random r;

  public RandomDimension(Class t, String id) {
    super(t, id);
    r = new Random();
  }

  @Override
  public int hashCode(Object o) {
    return r.nextInt();
  }
}
