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

package org.apache.hadoop.hive.ql.anon.perf;

import org.apache.hadoop.hive.ql.anon.gen.BodyGenerator;
import org.apache.hadoop.hive.ql.anon.gen.RowGeneratorFactory;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.IOException;

import static org.apache.hadoop.hive.ql.anon.consts.MsgConst.MSG_MSG_1;
import static org.apache.hadoop.hive.ql.anon.consts.MsgConst.MSG_MSG_3;

public class FileGenScenario1 implements FileGenerator {

  @Override
  public void genFile(RowWriter writer, final TestContext ctx, final int fileIx) throws IOException {
    final IntWritable msgId = new IntWritable();
    final LongWritable offset = new LongWritable();
    Writable body;

    final BodyGenerator generator = RowGeneratorFactory.getBodyGenerator(ctx.internalFormat);

    final long msgPerFile = ctx.totalMessages / ctx.numFiles;
    final int uniquesPerFile = ctx.numUniqueUsers / ctx.numFiles;
    int userCounter = 0;
    System.out.println("FIX: " + fileIx);
    for (int i = 0; i < msgPerFile; i++) {
      if (i % ctx.allToPiiRatio == 0) {
        final int userId = userCounter % uniquesPerFile + fileIx * uniquesPerFile;
        msgId.set(MSG_MSG_3);
        body = generator.generateMsg3(userId, ctx.piiMsgFieldLen);
        userCounter++;
      } else {
        msgId.set(MSG_MSG_1);
        body = generator.generateMsg1(i);
      }

      offset.set(i + fileIx * msgPerFile);
      final Writable row = writer.createRow(msgId, offset, body);
      writer.addRow(row);
    }
  }

}
