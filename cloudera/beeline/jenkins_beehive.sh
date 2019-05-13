#!/bin/bash
# The goal of this shell script to bootstrap cloudcat and cluster setup for Hive
# beeline tests, then running the tests on a cluster then destroy that cluster.
# Uses the same parameters as systests because it uses QE/infra_tools/utils.sh
# for common infrastructure needs.
set -ex

ACTIONS="clean setup"
AUTO_PAUSE="None"
JAVA_VERSION="8"
HDFS="basic"
KERBEROS="NONE"
SSL="false"
TLS="false"
JT_HA="false"
LICENSE="enterprise"
PARCELS="true"
CLOUDCAT_PROVISION="true"
DOMAIN="vpc.cloudera.com"
CLOUDCAT_EXPIRATION_DAYS="1"
JAVA8_BUILD=TRUE
if [ -z "$CLUSTER" ]
then
  CLUSTER_SHORTNAME="hive-beeline-test-""${RANDOM}""-{1..4}"
  NEW_CLUSTER=true
else
  CLUSTER_SHORTNAME=$CLUSTER
  NEW_CLUSTER=false
fi

HOSTS_LIST=($(eval echo ${CLUSTER_SHORTNAME}))

# Hiveserver should be on the first node
HIVESERVER2_NODE="${HOSTS_LIST[0]}.${DOMAIN}"
SSH_USER=jenkins

cd $WORKSPACE
# make the build tools available
. /opt/toolchain/toolchain.sh
echo "Note: utils.sh pulled from master branch"
curl -s -S -O --location https://github.mtv.cloudera.com/QE/infra_tools/raw/master/utils.sh
CDEP_ENV=1
# Convenience functions are imported from this file
# It should be the same as Cluster-Setup job
. utils.sh

if [ "$NEW_CLUSTER" = true ]
then
  # Create the hive safety valves
  OPTIONAL_ARGS="-is=HDFS,YARN,ZOOKEEPER,MAPREDUCE,HIVE,SPARK,SPARK_ON_YARN"
  OPTIONAL_ARGS="${OPTIONAL_ARGS} -jsonurl https://github.mtv.cloudera.com/raw/CDH/hive/cdh6.x/cloudera/beeline/hive-beeline.json"

  # Setup the cluster
  cloudcat_setup
fi

BEELINE_USER=hive
BEELINE_PASSWORD=

DATA_DIR=/run/cloudera-scm-agent
AUX_DIR=/home/systest

# Compiling hive
echo "Compiling hive..."
cd $WORKSPACE
mvn clean install -DskipTests -Phadoop-2,dist
echo "Compiling itests..."
cd $WORKSPACE/itests
mvn clean install -DskipTests -Phadoop-2

# Installing test data
cd $WORKSPACE
tar -cf data.tar data
scp -o UserKnownHostsFile=/dev/null \
    -o StrictHostKeyChecking=no \
    data.tar ${SSH_USER}@${HIVESERVER2_NODE}:/tmp/
ssh -o UserKnownHostsFile=/dev/null \
    -o StrictHostKeyChecking=no -q \
    ${SSH_USER}@${HIVESERVER2_NODE} << __EOF
    sudo su -
    mkdir -p ${DATA_DIR}
    cd ${DATA_DIR}
    tar -xf /tmp/data.tar
    chown systest:systest -R ${DATA_DIR}/data
    chmod a+rw -R ${DATA_DIR}/data
__EOF


for CLUSTER_NODE in "${HOSTS_LIST[@]}"
do
  # Upload hive-*.jar collection to the target machines
  ssh -o UserKnownHostsFile=/dev/null \
      -o StrictHostKeyChecking=no -q \
      ${SSH_USER}@${CLUSTER_NODE} << __EOF
      mkdir -p /tmp/hive-jars
      sudo chmod a+rwx ${AUX_DIR}
__EOF
  scp -o UserKnownHostsFile=/dev/null \
      -o StrictHostKeyChecking=no \
      $WORKSPACE/packaging/target/apache-hive-*-bin/apache-hive-*-bin/lib/hive*jar ${SSH_USER}@${CLUSTER_NODE}:/tmp/hive-jars
  ssh -o UserKnownHostsFile=/dev/null \
      -o StrictHostKeyChecking=no -q \
        ${SSH_USER}@${CLUSTER_NODE} \
      sudo cp /tmp/hive-jars/* /opt/cloudera/parcels/CDH/jars

  # Upload hive-it-util-*.jar to the target machines
  scp -o UserKnownHostsFile=/dev/null \
      -o StrictHostKeyChecking=no \
      $WORKSPACE/itests/util/target/hive-it-util-*.jar ${SSH_USER}@${CLUSTER_NODE}:${AUX_DIR}
done

# Restart cluster to read aux jars
set +e
echo "Restarting HIVE service"
WAIT_TIME=300
WAIT_TIME_DECREMENT=25
curl -X POST -u "admin:admin" -i http://${HIVESERVER2_NODE}:7180/api/v17/clusters/Cluster%201/services/HIVE-1/commands/restart
sleep $WAIT_TIME_DECREMENT
echo
echo "Waiting until HIVE service is ready"
timeout --foreground 0.5 bash -c "echo >\"/dev/tcp/${HIVESERVER2_NODE}/10000\"" >&/dev/null
PORT_CHECK=$?
while [ "$PORT_CHECK" -ne 0 ] && [ $WAIT_TIME -ge 0 ]
do
  echo "Sleeping ${WAIT_TIME_DECREMENT}s"
  sleep 5
  (( WAIT_TIME-=$WAIT_TIME_DECREMENT ))
  timeout --foreground 0.5 bash -c "echo >\"/dev/tcp/${HIVESERVER2_NODE}/10000\"" >&/dev/null
  PORT_CHECK=$?
done
set -e

# Load the property file, so we will no which tests to run
cd $WORKSPACE/cloudera/beeline
eval "$(awk -f readproperties.awk testconfiguration.properties)"

# Execute tests
# We execute them in set +e mode (which is not the default in jenkins) to ignore
# the exit code from the maven command.
if [ -n "${beeline_parallel}" ]
then
  echo "Running parallel qtests..."
  cd $WORKSPACE/itests/qtest
  set +e
  mvn clean test \
    -Phadoop-2 \
    -Dtest=TestBeeLineDriver \
    -Dtest.beeline.url="jdbc:hive2://${HIVESERVER2_NODE}:10000" \
    -Dtest.data.dir="${DATA_DIR}/data/files" \
    -Dtest.beeline.user="${BEELINE_USER}" \
    -Dtest.beeline.password="${BEELINE_PASSWORD}" \
    -Dmaven.test.redirectTestOutputToFile=true \
    -Djunit.parallel.timeout=3000 \
    -Dtest.results.dir=ql/src/test/results/clientpositive \
    -Dtest.init.script=q_test_init.sql \
    -Dtest.beeline.compare.portable=true \
    -Dtest.beeline.shared.database=false \
    -Djunit.parallel.threads=10 \
    -Dqfile="${beeline_parallel}"
  TEST_RESULT=$?
  set -e
  rm -rf target.parallel
  mv target target.parallel
else
  echo "Skipping parallel qtest, since not beeline_parallel is defined..."
fi

if [ -n "${beeline_sequential}" ]
then
  echo "Running sequential qtests..."
  set +e
  mvn clean test \
    -Phadoop-2 \
    -Dtest=TestBeeLineDriver \
    -Dtest.beeline.url="jdbc:hive2://${HIVESERVER2_NODE}:10000" \
    -Dtest.data.dir="${DATA_DIR}/data/files" \
    -Dtest.beeline.user="${BEELINE_USER}" \
    -Dtest.beeline.password="${BEELINE_PASSWORD}" \
    -Dmaven.test.redirectTestOutputToFile=true \
    -Djunit.parallel.timeout=3000 \
    -Dtest.results.dir=ql/src/test/results/clientpositive \
    -Dtest.init.script=q_test_init.sql \
    -Dtest.beeline.compare.portable=true \
    -Dtest.beeline.shared.database=true \
    -Djunit.parallel.threads=1 \
    -Dqfile="${beeline_sequential}"
  TEST_RESULT=$?
  rm -rf target.sequential
  mv target target.sequential
  set -e
else
  echo "Skipping sequential, since not beeline_sequential is defined..."
fi

cd $WORKSPACE/deploy/cdep
# Getting diagnostic bundle
ARGS=("--version=$CM_VERSION")
ARGS+=("--agents=$CDH+parcels@$CLUSTER_SHORTNAME.$DOMAIN")
ARGS+=("--no-locks")
collect_logs_into_workspace

# Finally destroy the cluster
if [[ $KEEP_CLUSTERS_ONLINE == "false" ]]; then
  ${CLOUDCAT_SCRIPT} --hosts=$CLUSTER_SHORTNAME.$DOMAIN \
      --log-dir=$WORKSPACE/cleanup_hosts_logs \
      --username=$CLOUDCAT_USERNAME destroy_group
fi
