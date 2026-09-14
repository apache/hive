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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.llap.io.encoded;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for {@link SerDeEncodedDataReader#buildSourceReaderJobConf(JobConf)}.
 *
 * <p>The LLAP encode path constructs its source {@link org.apache.hadoop.mapred.RecordReader}
 * via {@code sourceInputFormat.getRecordReader(split, buildSourceReaderJobConf(jobConf),
 * reporter)}. The clone must have vectorization disabled so that vectorized input formats
 * (e.g. {@link org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat}) pick their
 * row-per-{@code next()} branch, which is what {@link
 * SerDeEncodedDataReader.DeserializerOrcWriter#writeOneRow} expects.
 *
 * <p>These tests pin that contract without spinning up any LLAP infrastructure.
 */
public class TestSerDeEncodedDataReader {

  @Test
  public void clonedJobConfHasVectorizationDisabled() {
    JobConf original = new JobConf();
    HiveConf.setBoolVar(original, ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    original.setBoolean(Utilities.VECTOR_MODE, true);

    JobConf clone = SerDeEncodedDataReader.buildSourceReaderJobConf(original);

    assertNotSame("Helper must return a clone, not the original JobConf", original, clone);
    assertFalse("HIVE_VECTORIZATION_ENABLED must be false on the clone",
        HiveConf.getBoolVar(clone, ConfVars.HIVE_VECTORIZATION_ENABLED));
    assertFalse("Utilities.VECTOR_MODE must be false on the clone so that "
            + "Utilities.getIsVectorized(clone) short-circuits to false",
        clone.getBoolean(Utilities.VECTOR_MODE, true));
    assertFalse("Utilities.getIsVectorized must report false for the clone",
        Utilities.getIsVectorized(clone));
  }

  @Test
  public void originalJobConfIsNotMutated() {
    JobConf original = new JobConf();
    HiveConf.setBoolVar(original, ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    original.setBoolean(Utilities.VECTOR_MODE, true);

    SerDeEncodedDataReader.buildSourceReaderJobConf(original);

    assertTrue("Helper must not mutate the caller's JobConf: HIVE_VECTORIZATION_ENABLED",
        HiveConf.getBoolVar(original, ConfVars.HIVE_VECTORIZATION_ENABLED));
    assertTrue("Helper must not mutate the caller's JobConf: Utilities.VECTOR_MODE",
        original.getBoolean(Utilities.VECTOR_MODE, false));
  }

  @Test
  public void unrelatedConfigsArePropagated() {
    JobConf original = new JobConf();
    // A random unrelated key + a Hive config, to make sure the clone carries over context
    // that the downstream InputFormat may still need (e.g. hive.io.file.readcolumn.ids).
    original.set("test.unrelated.key", "kept");
    HiveConf.setVar(original, ConfVars.LLAP_IO_ENCODE_FORMATS,
        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");

    JobConf clone = SerDeEncodedDataReader.buildSourceReaderJobConf(original);

    assertEquals("Unrelated JobConf entries must survive the clone",
        "kept", clone.get("test.unrelated.key"));
    assertEquals("Unrelated Hive configs must survive the clone",
        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        HiveConf.getVar(clone, ConfVars.LLAP_IO_ENCODE_FORMATS));
  }

  /**
   * Guards the {@link Utilities#VECTOR_MODE} short-circuit inside
   * {@link Utilities#getIsVectorized(org.apache.hadoop.conf.Configuration)}: even when
   * {@code HIVE_VECTORIZATION_ENABLED} is left {@code true} on the original conf, setting
   * {@code VECTOR_MODE=false} on the clone must be enough to make
   * {@link org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat#getRecordReader}
   * pick the row-mode branch.
   */
  @Test
  public void vectorModeShortCircuitWinsOverHiveVectorizationEnabled() {
    JobConf original = new JobConf();
    HiveConf.setBoolVar(original, ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    // Deliberately do NOT set VECTOR_MODE on the original.

    JobConf clone = SerDeEncodedDataReader.buildSourceReaderJobConf(original);

    assertFalse("Even with only VECTOR_MODE flipped, getIsVectorized(clone) must be false",
        Utilities.getIsVectorized(clone));
  }
}
