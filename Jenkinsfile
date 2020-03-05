
def executorNode(run) {
  stage("An Executor") {
    container('maven') {
      run()
    }
  }
}


def testInParallel(parallelism, inclusionsFile, exclusionsFile, results, image, prepare, run) {
  //def splits
  executorNode {
    prepare()
//    splits = splitTests parallelism: parallelism, generateInclusions: true, estimateTestsFromFiles: true
  }
  def branches = [:]
  for (int i = 0; i < 3;/*splits.size()*/ i++) {
    def num = i
//    def split = splits[num]
    branches["split${num}"] = {
      stage("Test #${num + 1}") {
      executorNode {
        //docker.image(image).inside {
//          stage('Preparation') {
            prepare()
    //        writeFile file: (split.includes ? inclusionsFile : exclusionsFile), text: split.list.join("\n")
      //      writeFile file: (split.includes ? exclusionsFile : inclusionsFile), text: ''
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

pipeline {
  agent {
    kubernetes {
      yamlFile 'pod.yaml'
    }
  }
  stages {
    stage('Run maven') {
      steps {
        sh 'set'
        sh "echo OUTSIDE_CONTAINER_ENV_VAR = ${CONTAINER_ENV_VAR}"
        container('maven') {
          sh 'echo MAVEN_CONTAINER_ENV_VAR = ${CONTAINER_ENV_VAR}'
          sh 'mvn -version'
        }
        container('busybox') {
          sh 'echo BUSYBOX_CONTAINER_ENV_VAR = ${CONTAINER_ENV_VAR}'
          sh '/bin/busybox'
        }
      }
    }
  }
}
