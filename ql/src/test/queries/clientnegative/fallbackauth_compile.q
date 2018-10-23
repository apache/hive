set hive.security.authorization.enabled=true;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.fallback.FallbackHiveAuthorizerFactory;

compile `import org.apache.hadoop.hive.ql.exec.UDF \;
public class Pyth extends UDF {
  public double evaluate(double a, double b){
    return Math.sqrt((a*a) + (b*b)) \;
  }
} ` AS GROOVY NAMED Pyth.groovy;
