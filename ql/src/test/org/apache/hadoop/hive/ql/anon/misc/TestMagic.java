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

package org.apache.hadoop.hive.ql.anon.misc;

import org.apache.hadoop.hive.ql.anon.consts.BtreeConst;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMagic {

  @Test
  public void testBtMagic() {
    Assertions.assertEquals(0x4254, BtreeConst.genMagic("BT"));
    Assertions.assertEquals(0x4C4B, BtreeConst.genMagic("LK"));
    Assertions.assertEquals(0x5442, BtreeConst.genMagic("TB"));
  }

}
