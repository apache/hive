

pipeline {

agent any

stages {
  stage('Testing') {
    steps {
//      def splits=splitTests parallelism: parallelism, generateInclusions: true, estimateTestsFromFiles: true
      sh 'echo'
    }
    /*
    testInParallel(count(Integer.parseInt(params.SPLIT)), 'inclusions.txt', 'exclusions.txt', 'target/surefire-reports/TEST-*.xml', 'maven:3.5.0-jdk-8', {
  //    checkout scm
  //    unstash 'sources'
    }, {
      configFileProvider([configFile(fileId: 'artifactory', variable: 'SETTINGS')]) {
        withEnv(["MULTIPLIER=$params.MULTIPLIER"]) {
          sh 'mvn -s $SETTINGS -B install -Dmaven.test.failure.ignore -Dtest.groups= -pl common -am'
        }
      }
    })
    */
  }
}



//jenkins/jnlp-slave:3.27-1
}


