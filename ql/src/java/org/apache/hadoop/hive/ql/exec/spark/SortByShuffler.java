/**
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.io.BytesWritable;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.*;

public class SortByShuffler implements SparkShuffler {

  private final boolean totalOrder;

  /**
   * @param totalOrder whether this shuffler provides total order shuffle.
   */
  public SortByShuffler(boolean totalOrder) {
    this.totalOrder = totalOrder;
  }

  @Override
  public JavaPairRDD<HiveKey, Iterable<BytesWritable>> shuffle(
      JavaPairRDD<HiveKey, BytesWritable> input, int numPartitions) {
    JavaPairRDD<HiveKey, BytesWritable> rdd;
    if (totalOrder) {
      if (numPartitions > 0) {
        rdd = input.sortByKey(true, numPartitions);
      } else {
        rdd = input.sortByKey(true);
      }
    } else {
      Partitioner partitioner = new HashPartitioner(numPartitions);
      rdd = input.repartitionAndSortWithinPartitions(partitioner);
    }
    return rdd.mapPartitionsToPair(new ShuffleFunction());
  }

  private static class ShuffleFunction implements
      PairFlatMapFunction<Iterator<Tuple2<HiveKey, BytesWritable>>,
          HiveKey, Iterable<BytesWritable>> {
    // make eclipse happy
    private static final long serialVersionUID = 1L;

    @Override
    public Iterable<Tuple2<HiveKey, Iterable<BytesWritable>>> call(
        final Iterator<Tuple2<HiveKey, BytesWritable>> it) throws Exception {
      // Use input iterator to back returned iterable object.
      final Iterator<Tuple2<HiveKey, Iterable<BytesWritable>>> resultIt =
          new Iterator<Tuple2<HiveKey, Iterable<BytesWritable>>>() {
            HiveKey curKey = null;
            List<BytesWritable> curValues = new ArrayList<BytesWritable>();

            @Override
            public boolean hasNext() {
              return it.hasNext() || curKey != null;
            }

            @Override
            public Tuple2<HiveKey, Iterable<BytesWritable>> next() {
              // TODO: implement this by accumulating rows with the same key into a list.
              // Note that this list needs to improved to prevent excessive memory usage, but this
              // can be done in later phase.
              while (it.hasNext()) {
                Tuple2<HiveKey, BytesWritable> pair = it.next();
                if (curKey != null && !curKey.equals(pair._1())) {
                  HiveKey key = curKey;
                  List<BytesWritable> values = curValues;
                  curKey = pair._1();
                  curValues = new ArrayList<BytesWritable>();
                  curValues.add(pair._2());
                  return new Tuple2<HiveKey, Iterable<BytesWritable>>(key, values);
                }
                curKey = pair._1();
                curValues.add(pair._2());
              }
              if (curKey == null) {
                throw new NoSuchElementException();
              }
              // if we get here, this should be the last element we have
              HiveKey key = curKey;
              curKey = null;
              return new Tuple2<HiveKey, Iterable<BytesWritable>>(key, curValues);
            }

            @Override
            public void remove() {
              // Not implemented.
              // throw Unsupported Method Invocation Exception.
              throw new UnsupportedOperationException();
            }

          };

      return new Iterable<Tuple2<HiveKey, Iterable<BytesWritable>>>() {
        @Override
        public Iterator<Tuple2<HiveKey, Iterable<BytesWritable>>> iterator() {
          return resultIt;
        }
      };
    }
  }

}
