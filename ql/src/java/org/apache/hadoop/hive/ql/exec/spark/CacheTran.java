/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.hadoop.io.WritableComparable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.storage.StorageLevel;

public abstract class CacheTran<KI extends WritableComparable, VI, KO extends WritableComparable, VO>
  implements SparkTran<KI, VI, KO, VO> {
  // whether to cache current RDD.
  private boolean caching = false;
  private JavaPairRDD<KO, VO> cachedRDD;
  protected final String name;

  protected CacheTran(boolean cache, String name) {
    this.caching = cache;
    this.name = name;
  }

  @Override
  public JavaPairRDD<KO, VO> transform(
    JavaPairRDD<KI, VI> input) {
    if (caching) {
      if (cachedRDD == null) {
        cachedRDD = doTransform(input);
        cachedRDD.persist(StorageLevel.MEMORY_AND_DISK());
      }
      return cachedRDD.setName(this.name + " (" + cachedRDD.getNumPartitions() + ", cached)");
    } else {
      JavaPairRDD<KO, VO> rdd = doTransform(input);
      return rdd.setName(this.name + " (" + rdd.getNumPartitions() + ")");
    }
  }

  public Boolean isCacheEnable() {
    return caching;
  }

  protected abstract JavaPairRDD<KO, VO> doTransform(JavaPairRDD<KI, VI> input);

  @Override
  public String getName() {
    return name;
  }
}
