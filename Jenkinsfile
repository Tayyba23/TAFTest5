node {
    // Clean workspace before doing anything
  

    try {
        stage ('Clone') {
            checkout scm
            
        }
        stage ('Build') {
           
        }
       stage ('Tests') {
			def status = ""
            try{
              bat "java -jar target\\tafd.jar"
			  def out= "$JENKINS_HOME/jobs/$JOB_NAME/builds/${BUILD_NUMBER}"
				bat "cd $JENKINS_HOME/jobs/$JOB_NAME/builds/${BUILD_NUMBER} \n dir /b /a-d > tmp.txt"
				def files = readFile "$JENKINS_HOME/jobs/$JOB_NAME/builds/${BUILD_NUMBER}/tmp.txt"
				def temp="tmp.txt";
				bat "java -jar LogParser.jar $out temp.txt"
				status = readFile "$JENKINS_HOME/jobs/$JOB_NAME/builds/${BUILD_NUMBER}/result.txt"
				if(status.contains('Unsuccessful')){
					echo status
					throw err 
					}
					}
					catch(err)
					{
					currentBuild.result='UNSTABLE'
					}
					
        }
        stage ('Deploy') {	
            //update dashboard
            bat "echo 'will update dashbaord...'"
			echo "test"
        }
    } catch (err) {
        currentBuild.result = 'FAILED'
        throw err
    }
}
