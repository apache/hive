package org.apache.hadoop.hive.metastore.messaging.json;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.messaging.AddNotNullConstraintMessage;
import org.apache.thrift.TException;
import org.codehaus.jackson.annotate.JsonProperty;

public class JSONAddNotNullConstraintMessage extends AddNotNullConstraintMessage {
  @JsonProperty
  String server, servicePrincipal;

  @JsonProperty
  Long timestamp;

  @JsonProperty
  List<String> notNullConstraintListJson;

  /**
   * Default constructor, needed for Jackson.
   */
  public JSONAddNotNullConstraintMessage() {
  }

  public JSONAddNotNullConstraintMessage(String server, String servicePrincipal, List<SQLNotNullConstraint> nns,
      Long timestamp) {
    this.server = server;
    this.servicePrincipal = servicePrincipal;
    this.timestamp = timestamp;
    this.notNullConstraintListJson = new ArrayList<String>();
    try {
      for (SQLNotNullConstraint nn : nns) {
        notNullConstraintListJson.add(JSONMessageFactory.createNotNullConstraintObjJson(nn));
      }
    } catch (TException e) {
      throw new IllegalArgumentException("Could not serialize: ", e);
    }
  }

  @Override
  public String getServer() {
    return server;
  }

  @Override
  public String getServicePrincipal() {
    return servicePrincipal;
  }

  @Override
  public String getDB() {
    return null;
  }

  @Override
  public Long getTimestamp() {
    return timestamp;
  }

  @Override
  public List<SQLNotNullConstraint> getNotNullConstraints() throws Exception {
    List<SQLNotNullConstraint> nns = new ArrayList<SQLNotNullConstraint>();
    for (String nnJson : notNullConstraintListJson) {
      nns.add((SQLNotNullConstraint)JSONMessageFactory.getTObj(nnJson, SQLNotNullConstraint.class));
    }
    return nns;
  }

  @Override
  public String toString() {
    try {
      return JSONMessageDeserializer.mapper.writeValueAsString(this);
    } catch (Exception exception) {
      throw new IllegalArgumentException("Could not serialize: ", exception);
    }
  }
}
