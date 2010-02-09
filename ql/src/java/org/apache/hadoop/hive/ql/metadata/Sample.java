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

package org.apache.hadoop.hive.ql.metadata;

/**
 * A sample defines a subset of data based on sampling on a given dimension.
 * 
 **/
public class Sample {

  protected int sampleNum;
  protected int sampleFraction;
  protected Dimension sampleDimension;
  protected int moduloNum;

  @SuppressWarnings("nls")
  public Sample(int num, int fraction, Dimension d) throws HiveException {
    if ((num <= 0) || (num > fraction)) {
      throw new HiveException("Bad sample spec: " + num + "/" + fraction);
    }
    sampleNum = num;
    moduloNum = sampleNum - 1;
    sampleFraction = fraction;
    sampleDimension = d;
  }

  /**
   * Given an arbitrary object, determine if it falls within this sample.
   */
  public boolean inSample(Object o) {
    return (((sampleDimension.hashCode(o) & Integer.MAX_VALUE) % sampleFraction) == moduloNum);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null) {
      return false;
    }
    if (o instanceof Sample) {
      Sample s = (Sample) o;
      return ((sampleNum == s.sampleNum)
          && (sampleFraction == s.sampleFraction) && sampleDimension
          .equals(s.sampleDimension));
    }
    return (false);
  }

  public int getSampleNum() {
    return sampleNum;
  }

  public int getSampleFraction() {
    return sampleFraction;
  }

  public Dimension getSampleDimension() {
    return sampleDimension;
  }

  @SuppressWarnings("nls")
  @Override
  public String toString() {
    return sampleNum + "/" + sampleFraction + "@(" + sampleDimension + ")";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((sampleDimension == null) ? 0 : sampleDimension.hashCode());
    result = prime * result + sampleFraction;
    result = prime * result + sampleNum;
    return result;
  }
}
