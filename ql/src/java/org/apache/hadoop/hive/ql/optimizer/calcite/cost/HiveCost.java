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
package org.apache.hadoop.hive.ql.optimizer.calcite.cost;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptUtil;

/***
 * NOTE:<br>
 * 1. HiveCost normalizes cpu and io in to time.<br>
 * 2. CPU, IO cost is added together to find the query latency.<br>
 * 3. If query latency is equal then row count is compared.
 */

// TODO: This should inherit from VolcanoCost and should just override isLE
// method.
public class HiveCost implements RelOptCost {
  // ~ Static fields/initializers ---------------------------------------------

  public static final HiveCost                 INFINITY = new HiveCost(Double.POSITIVE_INFINITY,
                                                     Double.POSITIVE_INFINITY,
                                                     Double.POSITIVE_INFINITY) {
                                                   @Override
                                                   public String toString() {
                                                     return "{inf}";
                                                   }
                                                 };

  public static final HiveCost                 HUGE     = new HiveCost(Double.MAX_VALUE, Double.MAX_VALUE,
                                                     Double.MAX_VALUE) {
                                                   @Override
                                                   public String toString() {
                                                     return "{huge}";
                                                   }
                                                 };

  public static final HiveCost                 ZERO     = new HiveCost(0.0, 0.0, 0.0) {
                                                   @Override
                                                   public String toString() {
                                                     return "{0}";
                                                   }
                                                 };

  public static final HiveCost                 TINY     = new HiveCost(1.0, 1.0, 0.0) {
                                                   @Override
                                                   public String toString() {
                                                     return "{tiny}";
                                                   }
                                                 };

  public static final RelOptCostFactory FACTORY  = new Factory();

  // ~ Instance fields --------------------------------------------------------

  final double                          cpu;
  final double                          io;
  final double                          rowCount;

  // ~ Constructors -----------------------------------------------------------

  HiveCost(double rowCount, double cpu, double io) {
    assert rowCount >= 0d;
    assert cpu >= 0d;
    assert io >= 0d;
    this.rowCount = rowCount;
    this.cpu = cpu;
    this.io = io;
  }

  // ~ Methods ----------------------------------------------------------------

  public double getCpu() {
    return cpu;
  }

  public boolean isInfinite() {
    return (this == INFINITY) || (this.rowCount == Double.POSITIVE_INFINITY)
        || (this.cpu == Double.POSITIVE_INFINITY) || (this.io == Double.POSITIVE_INFINITY);
  }

  public double getIo() {
    return io;
  }

  public boolean isLe(RelOptCost other) {
    if ( (this.cpu + this.io < other.getCpu() + other.getIo()) ||
          ((this.cpu + this.io == other.getCpu() + other.getIo()) &&
          (this.rowCount <= other.getRows()))) {
      return true;
    }
    return false;
  }

  public boolean isLt(RelOptCost other) {
    return isLe(other) && !equals(other);
  }

  public double getRows() {
    return rowCount;
  }

  public boolean equals(RelOptCost other) {
    return (this == other) ||
            ((this.cpu + this.io == other.getCpu() + other.getIo()) &&
            (this.rowCount == other.getRows()));
  }

  public boolean isEqWithEpsilon(RelOptCost other) {
    return (this == other)
        || ((Math.abs(this.io - other.getIo()) < RelOptUtil.EPSILON)
            && (Math.abs(this.cpu - other.getCpu()) < RelOptUtil.EPSILON) && (Math
            .abs(this.rowCount - other.getRows()) < RelOptUtil.EPSILON));
  }

  public RelOptCost minus(RelOptCost other) {
    if (this == INFINITY) {
      return this;
    }

    return new HiveCost(this.rowCount - other.getRows(), this.cpu - other.getCpu(),
        this.io - other.getIo());
  }

  public RelOptCost multiplyBy(double factor) {
    if (this == INFINITY) {
      return this;
    }
    return new HiveCost(rowCount * factor, cpu * factor, io * factor);
  }

  public double divideBy(RelOptCost cost) {
    // Compute the geometric average of the ratios of all of the factors
    // which are non-zero and finite.
    double d = 1;
    double n = 0;
    if ((this.rowCount != 0) && !Double.isInfinite(this.rowCount) && (cost.getRows() != 0)
        && !Double.isInfinite(cost.getRows())) {
      d *= this.rowCount / cost.getRows();
      ++n;
    }
    if ((this.cpu != 0) && !Double.isInfinite(this.cpu) && (cost.getCpu() != 0)
        && !Double.isInfinite(cost.getCpu())) {
      d *= this.cpu / cost.getCpu();
      ++n;
    }
    if ((this.io != 0) && !Double.isInfinite(this.io) && (cost.getIo() != 0)
        && !Double.isInfinite(cost.getIo())) {
      d *= this.io / cost.getIo();
      ++n;
    }
    if (n == 0) {
      return 1.0;
    }
    return Math.pow(d, 1 / n);
  }

  public RelOptCost plus(RelOptCost other) {
    if ((this == INFINITY) || (other.isInfinite())) {
      return INFINITY;
    }
    return new HiveCost(this.rowCount + other.getRows(), this.cpu + other.getCpu(), this.io
        + other.getIo());
  }

  @Override
  public String toString() {
    return "{" + rowCount + " rows, " + cpu + " cpu, " + io + " io}";
  }

  private static class Factory implements RelOptCostFactory {
    private Factory() {
    }

    public RelOptCost makeCost(double rowCount, double cpu, double io) {
      return new HiveCost(rowCount, cpu, io);
    }

    public RelOptCost makeHugeCost() {
      return HUGE;
    }

    public HiveCost makeInfiniteCost() {
      return INFINITY;
    }

    public HiveCost makeTinyCost() {
      return TINY;
    }

    public HiveCost makeZeroCost() {
      return ZERO;
    }
  }
}
