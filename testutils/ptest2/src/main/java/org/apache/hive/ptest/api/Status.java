/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.ptest.api;

/**
 * Represents a generic status for a GenericResponse
 * in addition to the Status of a job.
 */
public class Status {
  private Name name;
  private String message;
  public Status() {

  }
  public Status(Name name, String message) {
    this.name = name;
    this.message = message;
  }
  public Name getName() {
    return name;
  }
  public void setName(Name name) {
    this.name = name;
  }
  public String getMessage() {
    return message;
  }
  public void setMessage(String message) {
    this.message = message;
  }

  @Override
  public String toString() {
    return "Status [name=" + name + ", message=" + message + "]";
  }

  public static enum Name {
    ILLEGAL_ARGUMENT(),
    QUEUE_FULL(),
    INTERNAL_ERROR(),
    PENDING(),
    IN_PROGRESS(),
    FAILED(),
    OK();
  }
  public static void assertOK(Status status) {
    if(!isOK(status)) {
      throw new RuntimeException(status == null ? "Status is null" : status.toString());
    }
  }
  public static void assertOKOrFailed(Status status) {
    if(!(isOK(status) || isFailed(status))) {
      throw new RuntimeException(status == null ? "Status is null" : status.toString());
    }
  }
  public static boolean isInProgress(Status status) {
    return status != null && Name.IN_PROGRESS.equals(status.getName());
  }
  public static boolean isPending(Status status) {
    return status != null && Name.PENDING.equals(status.getName());
  }
  public static boolean isOK(Status status) {
    return status != null && Name.OK.equals(status.getName());
  }
  public static boolean isIllegalArgument(Status status) {
    return status != null && Name.ILLEGAL_ARGUMENT.equals(status.getName());
  }
  public static boolean isFailed(Status status) {
    return status != null && Name.FAILED.equals(status.getName());
  }
  public static Status illegalArgument() {
    return illegalArgument(null);
  }
  public static Status illegalArgument(String message) {
    return new Status(Name.ILLEGAL_ARGUMENT, message);
  }
  public static Status queueFull() {
    return new Status(Name.QUEUE_FULL, null);
  }
  public static Status internalError(String message) {
    return new Status(Name.INTERNAL_ERROR, message);
  }
  public static Status pending() {
    return new Status(Name.PENDING, null);
  }
  public static Status inProgress() {
    return new Status(Name.IN_PROGRESS, null);
  }
  public static Status failed(String message) {
    return new Status(Name.FAILED, message);
  }
  public static Status ok() {
    return new Status(Name.OK, null);
  }
}
