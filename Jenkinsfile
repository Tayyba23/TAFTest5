node {
    // Clean workspace before doing anything
  

    try {
        stage ('Clone') {
            checkout scm
            
        }
        stage ('Build') {
           
        }
       stage ('Tests') {
            
               try{ bat "java -jar target\\tafd.jar"
				def out= "$JENKINS_HOME/jobs/$JOB_NAME/builds/${BUILD_NUMBER}"
				bat "cd $JENKINS_HOME/jobs/$JOB_NAME/builds/${BUILD_NUMBER} \n dir /b /a-d > tmp.txt"
				def files = readFile "$JENKINS_HOME/jobs/$JOB_NAME/builds/${BUILD_NUMBER}/tmp.txt"
				echo files
				def temp="tmp.txt";
				bat "java -jar LogParser.jar $out temp.txt"
				def status = readFile "$JENKINS_HOME/jobs/$JOB_NAME/builds/${BUILD_NUMBER}/result.txt"
					echo status
					
        }
        stage ('Deploy') {
            //update dashboard
            bat "echo 'shell scripts to deploy to server...'"
        }
    } catch (err) {
        currentBuild.result = 'FAILED'
        throw err
    }
}
