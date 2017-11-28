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
			  echo off
				def out= "$JENKINS_HOME/jobs/$JOB_NAME/builds/${BUILD_NUMBER}"
				bat "cd $JENKINS_HOME/jobs/$JOB_NAME/builds/${BUILD_NUMBER} \n dir /b /a-d > tmp.txt"
				def files = readFile "$JENKINS_HOME/jobs/$JOB_NAME/builds/${BUILD_NUMBER}/tmp.txt"
				echo files
				def temp="tmp.txt";
				bat "java -jar LogParser.jar $out temp.txt"
				status = readFile "$JENKINS_HOME/jobs/$JOB_NAME/builds/${BUILD_NUMBER}/result.txt"
			
				if(status.contains('Unsuccessful')){
				echo on
				echo "error"
				echo off
				}
				echo on
					echo status
					throw err 
					
					}
					catch(err)
					{
					currentBuild.result='UNSTABLE'
					}
					
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
