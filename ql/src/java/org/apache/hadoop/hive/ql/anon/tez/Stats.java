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

package org.apache.hadoop.hive.ql.anon.tez;

public class Stats {

  public int visitedMessages;
  public int piiMessages;
  public int policyMessages;
  public int anonymizedMessages;
  public int b;
  public long totalProcessingTime;
  public long seekTime;
  public long indexTime;
  public long totalAnonTime;
  public int jsonLength;
  public int numKeys;
  public String runnerName;
  public String colFormat;
  public int error;
  public int runNumber;

  public long matchesInspected;
  public long matchesRedacted;
  public long matchesFlagged;

  public long filesRewritten;

  public long start;
  public long end;

  public Stats() {
    start = System.nanoTime();
  }

  public void start() {
    start = System.nanoTime();
  }

  public void end(){
    end = System.nanoTime();
    totalProcessingTime = (end - start) / 1_000_000L;
  }

  public void clear() {
    visitedMessages = 0;
    piiMessages = 0;
    policyMessages = 0;
    anonymizedMessages = 0;
    b = 0;
    totalProcessingTime = 0;
    seekTime = 0;
    indexTime = 0;
    totalAnonTime = 0;
    jsonLength = 0;
    numKeys = 0;
    runnerName = "";
    colFormat = "";
    matchesInspected = 0;
    matchesRedacted = 0;
    matchesFlagged = 0;
    filesRewritten = 0;
  }

  public static void printHeader() {
    String hdr = String.format("%16s;%16s;%16s;%16s;%16s;%16s;%16s;%16s;%16s;%16s", "runner", "visited_msgs", "pii_msgs", "policy_msgs", "anon_msgs", "tot_anon_time", "tot_proc_time", "seek_time", "json_len", "num_keys");
    System.out.printf("%s\n", hdr);
  }

  @Override
  public String toString() {
    return String.format("%16s;%,16d;%,16d;%,16d;%,16d;%,16d;%,16d;%,16d;%,16d;%,16d;%d;%d",
      runnerName + "_" + colFormat, visitedMessages, piiMessages, policyMessages, anonymizedMessages, totalAnonTime, totalProcessingTime, seekTime, jsonLength, numKeys, runNumber, error);
  }
}
