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

import java.util.Iterator;

import org.apache.hadoop.io.BytesWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class SortByShuffler implements SparkShuffler {

  @Override
  public JavaPairRDD<BytesWritable, Iterable<BytesWritable>> shuffle(
      JavaPairRDD<BytesWritable, BytesWritable> input) {
    JavaPairRDD<BytesWritable, BytesWritable> rdd = input.sortByKey();
    return rdd.mapPartitionsToPair(new ShuffleFunction());
  };

  private static class ShuffleFunction implements
  PairFlatMapFunction<Iterator<Tuple2<BytesWritable, BytesWritable>>,
  BytesWritable, Iterable<BytesWritable>> {
    // make eclipse happy
    private static final long serialVersionUID = 1L;

    @Override
    public Iterable<Tuple2<BytesWritable, Iterable<BytesWritable>>> call(
        final Iterator<Tuple2<BytesWritable, BytesWritable>> it) throws Exception {
      // Use input iterator to back returned iterable object.
      final Iterator<Tuple2<BytesWritable, Iterable<BytesWritable>>> resultIt = 
          new Iterator<Tuple2<BytesWritable, Iterable<BytesWritable>>>() {
        BytesWritable curKey = null;
        BytesWritable curValue = null;

        @Override
        public boolean hasNext() {
          return it.hasNext() || curKey != null;
        }

        @Override
        public Tuple2<BytesWritable, Iterable<BytesWritable>> next() {
          // TODO: implement this by accumulating rows with the same key into a list.
          // Note that this list needs to improved to prevent excessive memory usage, but this
          // can be done in later phase.
          return null;
        }

        @Override
        public void remove() {
          // Not implemented.
          // throw Unsupported Method Invocation Exception.
        }

      };

      return new Iterable<Tuple2<BytesWritable, Iterable<BytesWritable>>>() {
        @Override
        public Iterator<Tuple2<BytesWritable, Iterable<BytesWritable>>> iterator() {
          return resultIt;
        }
      };
    }
  }

}
