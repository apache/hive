package org.apache.hadoop.hive.ql.plan.ptf;

import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PTFQueryInputType;

public class PTFQueryInputDef extends PTFInputDef {
  private String destination;
  private PTFQueryInputType type;

  public String getDestination() {
    return destination;
  }

  public void setDestination(String destination) {
    this.destination = destination;
  }

  public PTFQueryInputType getType() {
    return type;
  }

  public void setType(PTFQueryInputType type) {
    this.type = type;
  }

  @Override
  public PTFInputDef getInput() {
    return null;
  }
}