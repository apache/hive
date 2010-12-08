package org.apache.hadoop.hive.ql.parse;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.processors.SetProcessor;

public class VariableSubstitution {

  private static final Log l4j = LogFactory.getLog(VariableSubstitution.class);
  protected static Pattern varPat = Pattern.compile("\\$\\{[^\\}\\$\u0020]+\\}");
  protected static int MAX_SUBST = 40;

  public String substitute (HiveConf conf, String expr) {

    if (conf.getBoolVar(ConfVars.HIVEVARIABLESUBSTITUTE)){
      l4j.debug("Substitution is on: "+expr);
    } else {
      return expr;
    }
    if (expr == null) {
      return null;
    }
    Matcher match = varPat.matcher("");
    String eval = expr;
    for(int s=0; s<MAX_SUBST; s++) {
      match.reset(eval);
      if (!match.find()) {
        return eval;
      }
      String var = match.group();
      var = var.substring(2, var.length()-1); // remove ${ .. }
      String val = null;
      try {
        if (var.startsWith(SetProcessor.SYSTEM_PREFIX)) {
          val = System.getProperty(var.substring(SetProcessor.SYSTEM_PREFIX.length()));
        }
      } catch(SecurityException se) {
        l4j.warn("Unexpected SecurityException in Configuration", se);
      }
      if (val ==null){
        if (var.startsWith(SetProcessor.ENV_PREFIX)){
          val = System.getenv(var.substring(SetProcessor.ENV_PREFIX.length()));
        }
      }
      if (val == null) {
        if (var.startsWith(SetProcessor.HIVECONF_PREFIX)){
          val = conf.get(var.substring(SetProcessor.HIVECONF_PREFIX.length()));
        }
      }
      if (val == null) {
        l4j.debug("Interpolation result: "+eval);
        return eval; // return literal ${var}: var is unbound
      }
      // substitute
      eval = eval.substring(0, match.start())+val+eval.substring(match.end());
    }
    throw new IllegalStateException("Variable substitution depth too large: "
                                    + MAX_SUBST + " " + expr);
  }
}
