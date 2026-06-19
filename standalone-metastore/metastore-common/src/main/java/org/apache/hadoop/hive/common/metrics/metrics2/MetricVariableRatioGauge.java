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
package org.apache.hadoop.hive.common.metrics.metrics2;

import com.codahale.metrics.RatioGauge;
import org.apache.hadoop.hive.common.metrics.common.MetricsVariable;

/**
 * Combines two numeric metric variables into one gauge type metric displaying their ratio
 */
public class MetricVariableRatioGauge extends RatioGauge {

  private final MetricsVariable<Integer> numerator;
  private final MetricsVariable<Integer> denominator;

  public MetricVariableRatioGauge(MetricsVariable<Integer> numerator,
                                      MetricsVariable<Integer> denominator) {
    this.numerator = numerator;
    this.denominator = denominator;
  }

  @Override
  protected Ratio getRatio() {
    Integer numValue = numerator.getValue();
    Integer denomValue = denominator.getValue();
    if(numValue != null && denomValue != null) {
      return Ratio.of(numValue.doubleValue(), denomValue.doubleValue());
    }
    return Ratio.of(0d,0d);
  }
}
