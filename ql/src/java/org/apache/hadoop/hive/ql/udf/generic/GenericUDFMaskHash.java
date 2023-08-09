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

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Description(name = "mask_hash",
             value = "returns hash of the given value",
             extended = "Examples:\n "
                      + "  mask_hash(value)\n "
                      + "Arguments:\n "
                      + "  value - value to mask. Supported types: STRING, VARCHAR, CHAR"
            )
public class GenericUDFMaskHash extends BaseMaskUDF {
  public static final String UDF_NAME = "mask_hash";

  public void configure(MapredContext context) {
    boolean isSha512 =
        "sha512".equalsIgnoreCase(HiveConf.getVar(context.getJobConf(), HiveConf.ConfVars.HIVE_MASKING_ALGO).trim());
    ((MaskHashTransformer) transformer).setSHA512(isSha512);
  }

  public GenericUDFMaskHash() {
    super(new MaskHashTransformer(), UDF_NAME);
  }
}

class MaskHashTransformer extends AbstractTransformer {

  private static final Logger LOG = LoggerFactory.getLogger(MaskHashTransformer.class);

  private boolean isSHA512 = false;
  @Override
  public void init(ObjectInspector[] arguments, int startIdx) {
  }

  public void setSHA512(boolean val) {
    if (val) {
      LOG.info("Using SHA512 for masking");
    } else {
      LOG.info("Using SHA256 for masking");
    }
    this.isSHA512 = val;
  }

  @Override
  String transform(final String value) {
    if (getIsSHA512FromSessionConf() || isSHA512) {
      return DigestUtils.sha512Hex(value);
    } else {
      return DigestUtils.sha256Hex(value);
    }
  }

  private boolean getIsSHA512FromSessionConf() {
    if (SessionState.get() != null) {
      return "sha512".equalsIgnoreCase(
          HiveConf.getVar(SessionState.get().getConf(), HiveConf.ConfVars.HIVE_MASKING_ALGO).trim());
    }
    return false;
  }

  @Override
  Byte transform(final Byte value) {
    return null;
  }

  @Override
  Short transform(final Short value) {
    return null;
  }

  @Override
  Integer transform(final Integer value) {
    return null;
  }

  @Override
  Long transform(final Long value) {
    return null;
  }

  @Override
  Date transform(final Date value) {
    return null;
  }
}
