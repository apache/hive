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
package org.apache.hadoop.hive.druid.json;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.StringUtils;
import org.joda.time.Duration;
import org.joda.time.Period;

import java.util.Map;

/**
 * This class is copied from druid source code
 * in order to avoid adding additional dependencies on druid-indexing-service.
 */
public class KafkaSupervisorIOConfig {
  public static final String BOOTSTRAP_SERVERS_KEY = "bootstrap.servers";

  private final String topic;
  private final Integer replicas;
  private final Integer taskCount;
  private final Duration taskDuration;
  private final Map<String, String> consumerProperties;
  private final Duration startDelay;
  private final Duration period;
  private final boolean useEarliestOffset;
  private final Duration completionTimeout;
  @SuppressWarnings({ "OptionalUsedAsFieldOrParameterType", "Guava" }) private final Optional<Duration>
      lateMessageRejectionPeriod;
  @SuppressWarnings({ "OptionalUsedAsFieldOrParameterType", "Guava" }) private final Optional<Duration>
      earlyMessageRejectionPeriod;
  private final boolean skipOffsetGaps;

  @JsonCreator public KafkaSupervisorIOConfig(@JsonProperty("topic") String topic,
      @JsonProperty("replicas") Integer replicas,
      @JsonProperty("taskCount") Integer taskCount,
      @JsonProperty("taskDuration") Period taskDuration,
      @JsonProperty("consumerProperties") Map<String, String> consumerProperties,
      @JsonProperty("startDelay") Period startDelay,
      @JsonProperty("period") Period period,
      @JsonProperty("useEarliestOffset") Boolean useEarliestOffset,
      @JsonProperty("completionTimeout") Period completionTimeout,
      @JsonProperty("lateMessageRejectionPeriod") Period lateMessageRejectionPeriod,
      @JsonProperty("earlyMessageRejectionPeriod") Period earlyMessageRejectionPeriod,
      @JsonProperty("skipOffsetGaps") Boolean skipOffsetGaps) {
    this.topic = Preconditions.checkNotNull(topic, "topic");
    this.consumerProperties = Preconditions.checkNotNull(consumerProperties, "consumerProperties");
    Preconditions.checkNotNull(consumerProperties.get(BOOTSTRAP_SERVERS_KEY),
        StringUtils.format("consumerProperties must contain entry for [%s]", BOOTSTRAP_SERVERS_KEY));

    this.replicas = replicas != null ? replicas : 1;
    this.taskCount = taskCount != null ? taskCount : 1;
    this.taskDuration = defaultDuration(taskDuration, "PT1H");
    this.startDelay = defaultDuration(startDelay, "PT5S");
    this.period = defaultDuration(period, "PT30S");
    this.useEarliestOffset = useEarliestOffset != null ? useEarliestOffset : false;
    this.completionTimeout = defaultDuration(completionTimeout, "PT30M");
    //noinspection Guava
    this.lateMessageRejectionPeriod =
        lateMessageRejectionPeriod == null ?
            Optional.absent() :
            Optional.of(lateMessageRejectionPeriod.toStandardDuration());
    //noinspection Guava
    this.earlyMessageRejectionPeriod =
        earlyMessageRejectionPeriod == null ?
            Optional.absent() :
            Optional.of(earlyMessageRejectionPeriod.toStandardDuration());
    this.skipOffsetGaps = skipOffsetGaps != null ? skipOffsetGaps : false;
  }

  @JsonProperty public String getTopic() {
    return topic;
  }

  @JsonProperty public Integer getReplicas() {
    return replicas;
  }

  @JsonProperty public Integer getTaskCount() {
    return taskCount;
  }

  @JsonProperty public Duration getTaskDuration() {
    return taskDuration;
  }

  @JsonProperty public Map<String, String> getConsumerProperties() {
    return consumerProperties;
  }

  @JsonProperty public Duration getStartDelay() {
    return startDelay;
  }

  @JsonProperty public Duration getPeriod() {
    return period;
  }

  @JsonProperty public boolean isUseEarliestOffset() {
    return useEarliestOffset;
  }

  @JsonProperty public Duration getCompletionTimeout() {
    return completionTimeout;
  }

  @SuppressWarnings("Guava") @JsonProperty public Optional<Duration> getEarlyMessageRejectionPeriod() {
    return earlyMessageRejectionPeriod;
  }

  @SuppressWarnings("Guava") @JsonProperty public Optional<Duration> getLateMessageRejectionPeriod() {
    return lateMessageRejectionPeriod;
  }

  @JsonProperty public boolean isSkipOffsetGaps() {
    return skipOffsetGaps;
  }

  @Override public String toString() {
    return "KafkaSupervisorIOConfig{"
        + "topic='"
        + topic
        + '\''
        + ", replicas="
        + replicas
        + ", taskCount="
        + taskCount
        + ", taskDuration="
        + taskDuration
        + ", consumerProperties="
        + consumerProperties
        + ", startDelay="
        + startDelay
        + ", period="
        + period
        + ", useEarliestOffset="
        + useEarliestOffset
        + ", completionTimeout="
        + completionTimeout
        + ", lateMessageRejectionPeriod="
        + lateMessageRejectionPeriod
        + ", skipOffsetGaps="
        + skipOffsetGaps
        + '}';
  }

  private static Duration defaultDuration(final Period period, final String theDefault) {
    return (period == null ? new Period(theDefault) : period).toStandardDuration();
  }

  @Override public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    KafkaSupervisorIOConfig that = (KafkaSupervisorIOConfig) o;

    if (useEarliestOffset != that.useEarliestOffset) {
      return false;
    }
    if (skipOffsetGaps != that.skipOffsetGaps) {
      return false;
    }
    if (topic != null ? !topic.equals(that.topic) : that.topic != null) {
      return false;
    }
    if (replicas != null ? !replicas.equals(that.replicas) : that.replicas != null) {
      return false;
    }
    if (taskCount != null ? !taskCount.equals(that.taskCount) : that.taskCount != null) {
      return false;
    }
    if (taskDuration != null ? !taskDuration.equals(that.taskDuration) : that.taskDuration != null) {
      return false;
    }
    if (consumerProperties != null ?
        !consumerProperties.equals(that.consumerProperties) :
        that.consumerProperties != null) {
      return false;
    }
    if (startDelay != null ? !startDelay.equals(that.startDelay) : that.startDelay != null) {
      return false;
    }
    if (period != null ? !period.equals(that.period) : that.period != null) {
      return false;
    }
    if (completionTimeout != null ?
        !completionTimeout.equals(that.completionTimeout) :
        that.completionTimeout != null) {
      return false;
    }
    if (lateMessageRejectionPeriod.isPresent() ?
        !lateMessageRejectionPeriod.equals(that.lateMessageRejectionPeriod) :
        that.lateMessageRejectionPeriod.isPresent()) {
      return false;
    }
    return earlyMessageRejectionPeriod.isPresent() ?
        earlyMessageRejectionPeriod.equals(that.earlyMessageRejectionPeriod) :
        !that.earlyMessageRejectionPeriod.isPresent();
  }

  @Override public int hashCode() {
    int result = topic != null ? topic.hashCode() : 0;
    result = 31 * result + (replicas != null ? replicas.hashCode() : 0);
    result = 31 * result + (taskCount != null ? taskCount.hashCode() : 0);
    result = 31 * result + (taskDuration != null ? taskDuration.hashCode() : 0);
    result = 31 * result + (consumerProperties != null ? consumerProperties.hashCode() : 0);
    result = 31 * result + (startDelay != null ? startDelay.hashCode() : 0);
    result = 31 * result + (period != null ? period.hashCode() : 0);
    result = 31 * result + (useEarliestOffset ? 1 : 0);
    result = 31 * result + (completionTimeout != null ? completionTimeout.hashCode() : 0);
    result = 31 * result + (lateMessageRejectionPeriod.isPresent() ? lateMessageRejectionPeriod.hashCode() : 0);
    result = 31 * result + (earlyMessageRejectionPeriod.isPresent() ? earlyMessageRejectionPeriod.hashCode() : 0);
    result = 31 * result + (skipOffsetGaps ? 1 : 0);
    return result;
  }
}
