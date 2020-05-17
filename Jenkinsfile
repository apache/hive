
properties([
    // max 5 build/branch/day
//    rateLimitBuilds(throttle: [count: 5, durationName: 'day', userBoost: true]),
    // do not run multiple testruns on the same branch
//    disableConcurrentBuilds(),
    parameters([
        string(name: 'SPLIT', defaultValue: '20', description: 'Number of buckets to split tests into.'),
        string(name: 'OPTS', defaultValue: '', description: 'additional maven opts'),
        string(name: 'SCRIPT', defaultValue: '', description: 'custom build script'),
    ])
])

def setPrLabel(String prLabel) {
  if (env.CHANGE_ID) {
   def mapping=[
    "SUCCESS":"tests passed",
    "UNSTABLE":"tests unstable",
    "FAILURE":"tests failed",
    "PENDING":"tests pending",
   ]
   def newLabels = []
   for( String l : pullRequest.labels )
     newLabels.add(l)
   for( String l : mapping.keySet() )
     newLabels.remove(mapping[l])
   newLabels.add(mapping[prLabel])
   echo ('' +newLabels)
   pullRequest.labels=newLabels
  }
}

setPrLabel("PENDING");

def executorNode(run) {
  hdbPodTemplate {
      node(POD_LABEL) {
        container('hdb') {
          run()
        }
    }
  }
}

def buildHive(args) {
  configFileProvider([configFile(fileId: 'artifactory', variable: 'SETTINGS')]) {
    withEnv(["MULTIPLIER=$params.MULTIPLIER","M_OPTS=$params.OPTS"]) {
      sh '''#!/bin/bash -e
ls -l
set -x
. /etc/profile.d/confs.sh
export USER="`whoami`"
#export MAVEN_OPTS="-Xmx1333m"
export -n HIVE_CONF_DIR
#export HIVE_HOME="$PWD"
OPTS=" -s $SETTINGS -B -Dmaven.test.failure.ignore -Dtest.groups= "
OPTS+=" -Pitests,qsplits"
OPTS+=" -Dorg.slf4j.simpleLogger.log.org.apache.maven.plugin.surefire.SurefirePlugin=INFO"
OPTS+=" -Dmaven.repo.local=$PWD/.m2"
OPTS+=" $M_OPTS "
if [ -s inclusions.txt ]; then OPTS+=" -Dsurefire.includesFile=$PWD/inclusions.txt";fi
if [ -s exclusions.txt ]; then OPTS+=" -Dsurefire.excludesFile=$PWD/exclusions.txt";fi
#cd hive
mvn $OPTS '''+args+'''
du -h --max-depth=1
'''
    }
  }
}

def rsyncPodTemplate(closure) {
  podTemplate(
  containers: [
    containerTemplate(name: 'rsync', image: 'kgyrtkirk/htk-rsync:latest', ttyEnabled: true,
        alwaysPullImage: true,
        resourceRequestCpu: '300m',
        resourceLimitCpu: '1300m',
        resourceRequestMemory: '250Mi',
    ),
  ]) {
    closure();
  }
}

def hdbPodTemplate(closure) {
  podTemplate(
  containers: [
    containerTemplate(name: 'hdb', image: 'kgyrtkirk/hive-dev-box:executor', ttyEnabled: true, command: 'cat',
        alwaysPullImage: true,
        resourceRequestCpu: '1000m',
        resourceRequestMemory: '6500Mi',
        resourceLimitMemory: '12000Mi'
    ),
  ], yaml:'''
spec:
  securityContext:
    fsGroup: 1000
  tolerations:
    - key: "type"
      operator: "Equal"
      value: "slave"
      effect: "PreferNoSchedule"
    - key: "type"
      operator: "Equal"
      value: "slave"
      effect: "NoSchedule"
  nodeSelector:
    type: slave
''') {
    closure();
  }
}

def jobWrappers(closure) {
  try {
    // allocate 1 precommit token for the execution
    lock(label:'hive-precommit',quantity:1)  {
      timestamps {
        rsyncPodTemplate {
          node(POD_LABEL) {
            // launch the "rsync" container to store build data
            container('rsync') {
              stage('Prepare rsync') {
                sh '''printf 'env.S="%s"' "`hostname -i`" | tee load.props'''
                load 'load.props'
                sh 'df -h /data'
              }
            }
            closure()
          }
        }
      }
    }
  } finally {
    setPrLabel(currentBuild.currentResult)
  }
}

jobWrappers {

  def splits
  executorNode {
    container('hdb') {
      stage('Checkout') {
        checkout scm
        // why dup?
        sh '''#!/bin/bash -e
            # make parallel-test-execution plugins source scanner happy ~ better results for 1st run
            find . -name '*.java'|grep /Test|grep -v src/test/java|grep org/apache|while read f;do t="`echo $f|sed 's|.*org/apache|happy/src/test/java/org/apache|'`";mkdir -p  "${t%/*}";touch "$t";done
        '''
      }
      stage('Compile') {
        buildHive("install -Dtest=noMatches")
        sh '''#!/bin/bash -e
            # make parallel-test-execution plugins source scanner happy ~ better results for 1st run
            find . -name '*.java'|grep /Test|grep -v src/test/java|grep org/apache|while read f;do t="`echo $f|sed 's|.*org/apache|happy/src/test/java/org/apache|'`";mkdir -p  "${t%/*}";touch "$t";done
        '''
      }
      stage('Upload') {
        sh  'rsync -rltDq --stats . rsync://$S/data'

        splits = splitTests parallelism: count(Integer.parseInt(params.SPLIT)), generateInclusions: true, estimateTestsFromFiles: true
      }
    }
  }

  stage('Testing') {

    def branches = [:]
    for (int i = 0; i < splits.size(); i++) {
      def num = i
      def split = splits[num]
      def splitName=String.format("split-%02d",num+1)
      branches[splitName] = {
        executorNode {
          stage('Prepare') {
              sh  'rsync -rltDq --stats rsync://$S/data .'
              writeFile file: (split.includes ? "inclusions.txt" : "exclusions.txt"), text: split.list.join("\n")
              writeFile file: (split.includes ? "exclusions.txt" : "inclusions.txt"), text: ''
              sh '''echo "@INC";cat inclusions.txt;echo "@EXC";cat exclusions.txt;echo "@END"'''
          }
          try {
            stage('Test') {
              buildHive("install -q")
            }
          } finally {
            stage('Archive') {
              junit '**/TEST-*.xml'
            }
          }
        }
      }
    }
    parallel branches
  }
}
