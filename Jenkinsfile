def executorNode(run) {
  stage("An Executor") {
    node(POD_LABEL) {
      container('maven') {
        timestamps {
          run()
        }
      }
    }
  }
}

def testInParallel(parallelism, inclusionsFile, exclusionsFile, results, image, prepare, run) {
  def splits
  node {
    splits = splitTests parallelism: parallelism, generateInclusions: true, estimateTestsFromFiles: false
  }
  def branches = [:]
  for (int i = 0; i < splits.size(); i++) {
    def num = i
    def split = splits[num]
    branches["split${num}"] = {
      stage("Test #${num + 1}") {
      executorNode {
        //docker.image(image).inside {
//          stage('Preparation') {
            prepare()
            writeFile file: (split.includes ? inclusionsFile : exclusionsFile), text: split.list.join("\n")
            writeFile file: (split.includes ? exclusionsFile : inclusionsFile), text: ''
  //        }
        //  stage('Main') {
            realtimeJUnit(results) {
              run()
            }
//          }
        }
      }
    }
  }
  parallel branches
}


podTemplate(workspaceVolume: dynamicPVC(requestsSize: "16Gi"), containers: [
    containerTemplate(name: 'maven', image: 'cloudbees/jnlp-slave-with-java-build-tools', ttyEnabled: true, command: 'cat',
        resourceRequestCpu: '1500m',
        resourceLimitCpu: '4000m',
        resourceRequestMemory: '3000Mi',
        resourceLimitMemory: '8000Mi'
    ),
//    containerTemplate(name: 'maven', image: 'maven:3.3.9-jdk-8-alpine', ttyEnabled: true, command: 'cat'),
//    containerTemplate(name: 'golang', image: 'golang:1.8.0', ttyEnabled: true, command: 'cat')
  ], yaml:'''
spec:
  securityContext:
    fsGroup: 1000
''') {

properties([
    parameters([
        string(name: 'MULTIPLIER', defaultValue: '1', description: 'Factor by which to artificially slow down tests.'),
        string(name: 'SPLIT', defaultValue: '5', description: 'Number of buckets to split tests into.'),
        string(name: 'OPTS', defaultValue: '', description: 'additional maven opts')
    ])
])



/*
node(POD_LABEL) {
  container('maven') {
  	sh 'ls -l'
  }
}
*/

stage('Testing') {
  testInParallel(count(Integer.parseInt(params.SPLIT)), 'inclusions.txt', 'exclusions.txt', '**/target/surefire-reports/TEST-*.xml', 'maven:3.5.0-jdk-8', {
//        sh 'git clone -b pipe1 https://github.com/kgyrtkirk/pipeline-test'
    checkout scm
//    unstash 'sources'
  }, {
    configFileProvider([configFile(fileId: 'artifactory', variable: 'SETTINGS')]) {
      withEnv(["MULTIPLIER=$params.MULTIPLIER","M_OPTS=$params.OPTS"]) {
      stage('build1') {
        sh '''#!/bin/bash -e
OPTS=" -s $SETTINGS -B install -Dmaven.test.failure.ignore -Dtest.groups= "
OPTS+="-pl ql -am "
OPTS+=" -Dmaven.repo.local=$PWD/.m2"
OPTS+=" $M_OPTS "
mvn $OPTS
'''
      }
      stage('build2') {
        sh '''#!/bin/bash -e
OPTS=" -s $SETTINGS -B install -Dmaven.test.failure.ignore -Dtest.groups= "
OPTS+="-pl ql -am "
OPTS+=" -Dmaven.repo.local=$PWD/.m2"
OPTS+=" $M_OPTS "
mvn $OPTS
'''
      }
      stage('test1') {
        sh '''#!/bin/bash -e
OPTS=" -s $SETTINGS -B test -Dmaven.test.failure.ignore -Dtest.groups= "
OPTS+="-pl ql -am "
OPTS+=" -Dmaven.repo.local=$PWD/.m2"
OPTS+=" $M_OPTS "
mvn $OPTS
'''
      }
      }

      withEnv(["MULTIPLIER=$params.MULTIPLIER","M_OPTS=$params.OPTS"]) {
        sh '''#!/bin/bash -e
OPTS=" -s $SETTINGS -B install -Dmaven.test.failure.ignore -Dtest.groups= "
#OPTS+="-pl ql -am "
if [ -s inclusions.txt ]; then OPTS+=" -Dsurefire.includesFile=$PWD/inclusions.txt";fi
if [ -s exclusions.txt ]; then OPTS+=" -Dsurefire.excludesFile=$PWD/exclusions.txt";fi
OPTS+=" -Dmaven.repo.local=$PWD/.m2"
OPTS+=" $M_OPTS "
mvn $OPTS
du -h --max-depth=1
'''
      }
    }
  })
}



//jenkins/jnlp-slave:3.27-1
}


