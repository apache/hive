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
package org.apache.hadoop.hive.ql.udf.esri;

import com.esri.core.geometry.Envelope;

public class BinUtils {
  final long numCols;
  final double extentMin;
  final double extentMax;
  final double binSize;

  public BinUtils(double binSize) {
    this.binSize = binSize;

    // absolute max number of rows/columns we can have
    long maxBinsPerAxis = (long) Math.sqrt(Long.MAX_VALUE);

    // a smaller binSize gives us a smaller extent width and height that
    // can be addressed by a single 64 bit long
    double size = (binSize < 1) ? maxBinsPerAxis * binSize : maxBinsPerAxis;

    extentMax = size / 2;
    extentMin = extentMax - size;
    numCols = (long) (Math.ceil(size / binSize));
  }

  /**
   * Gets bin ID from a point.
   *
   * @param x
   * @param y
   * @return
   */
  public long getId(double x, double y) {
    double down = (extentMax - y) / binSize;
    double over = (x - extentMin) / binSize;

    return ((long) down * numCols) + (long) over;
  }

  /**
   * Gets the envelope for the bin ID.
   *
   * @param binId
   * @param envelope
   */
  public void queryEnvelope(long binId, Envelope envelope) {
    long down = binId / numCols;
    long over = binId % numCols;

    double xmin = extentMin + (over * binSize);
    double xmax = xmin + binSize;
    double ymax = extentMax - (down * binSize);
    double ymin = ymax - binSize;

    envelope.setCoords(xmin, ymin, xmax, ymax);
  }

  /**
   * Gets the envelope for the bin that contains the x,y coords.
   *
   * @param x
   * @param y
   * @param envelope
   */
  public void queryEnvelope(double x, double y, Envelope envelope) {
    double down = (extentMax - y) / binSize;
    double over = (x - extentMin) / binSize;

    double xmin = extentMin + (over * binSize);
    double xmax = xmin + binSize;
    double ymax = extentMax - (down * binSize);
    double ymin = ymax - binSize;

    envelope.setCoords(xmin, ymin, xmax, ymax);
  }
}
