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

package org.apache.hadoop.hive.ql.parse.repl.dump.io;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class ConstraintsSerializer implements JsonWriter.Serializer {
  private HiveConf hiveConf;
  private List<SQLPrimaryKey> pks;
  private List<SQLForeignKey> fks;
  private List<SQLUniqueConstraint> uks;
  private List<SQLNotNullConstraint> nns;
  private List<SQLDefaultConstraint> dks;
  private List<SQLCheckConstraint> cks;

  public ConstraintsSerializer(List<SQLPrimaryKey> pks, List<SQLForeignKey> fks,
      List<SQLUniqueConstraint> uks, List<SQLNotNullConstraint> nns, List<SQLDefaultConstraint> dks,
      List<SQLCheckConstraint> cks, HiveConf hiveConf) {
    this.hiveConf = hiveConf;
    this.pks = pks;
    this.fks = fks;
    this.uks = uks;
    this.nns = nns;
    this.dks = dks;
    this.cks = cks;
  }

  @Override
  public void writeTo(JsonWriter writer, ReplicationSpec additionalPropertiesProvider)
      throws SemanticException, IOException {
    String pksString, fksString, uksString, nnsString, dksString, cksString;
    pksString = fksString = uksString = nnsString = dksString = cksString = "";
    if (pks != null) {
      pksString = MessageBuilder.getInstance().buildAddPrimaryKeyMessage(pks).toString();
    }
    if (fks != null) {
      fksString = MessageBuilder.getInstance().buildAddForeignKeyMessage(fks).toString();
    }
    if (uks != null) {
      uksString = MessageBuilder.getInstance().buildAddUniqueConstraintMessage(uks).toString();
    }
    if (nns != null) {
      nnsString = MessageBuilder.getInstance().buildAddNotNullConstraintMessage(nns).toString();
    }
    if (dks != null) {
      dksString = MessageBuilder.getInstance().buildAddDefaultConstraintMessage(dks).toString();
    }
    if (cks != null) {
      cksString = MessageBuilder.getInstance().buildAddCheckConstraintMessage(cks).toString();
    }
    writer.jsonGenerator.writeStringField("pks", pksString);
    writer.jsonGenerator.writeStringField("uks", uksString);
    writer.jsonGenerator.writeStringField("nns", nnsString);
    writer.jsonGenerator.writeStringField("fks", fksString);
    writer.jsonGenerator.writeStringField("dks", dksString);
    writer.jsonGenerator.writeStringField("cks", cksString);
  }
}
