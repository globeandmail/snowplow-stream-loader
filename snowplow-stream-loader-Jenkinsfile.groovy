def label = "worker-${UUID.randomUUID().toString()}"
isGitPush = []
isManualMode = false
def notifyStarted() {
    slackSend(
            color: 'warning',
            message: "STARTED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})",
    )
}
​
def notifySuccess() {
    slackSend(
            color: 'good',
            message: "SUCCESS: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})",
    )
}
​
def notifyFailed(Exception e) {
    slackSend(
            color: 'danger',
            message: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL}): ${e.toString()}"
    )
    ​
    throw e
}
​
def checkoutCurrentRepo() {
    def vars = checkout scm
    ​
    return vars
}
​
def getFileChangesFromApi(String prId) {
    def jsonResp
    def fileChanges
    def fileNames
    ​
    withCredentials([string(credentialsId: 'github-token', variable: 'GITHUB_TOKEN')]) {
        jsonResp = sh(
                script: "wget --header 'Authorization: Bearer ${env.GITHUB_TOKEN}' -qO- https://api.github.com/repos/globeandmail/snowplow-stream-loader/pulls/${env.CHANGE_ID}/files",
                returnStdout: true
        )
        fileChanges = readJSON(text: jsonResp)
        fileNames = fileChanges.collect { it.filename }
        echo "File names are ${fileNames.toString()}"
    }
    return fileNames.join("\n")
}
​
def extractAppsToDeploy(String fileChanges) {
    echo "file changes are ${fileChanges}"
    def files = fileChanges.split('\n')
    def applications = []
    files.each { file ->
        def path = "${file}".split('/')
        //If build.yaml is manually changed then dont bump version automatically
        if(path[-1]=="build.yaml"){
            echo "Revert Mode detected, automatic version bump disabled"
            isManualMode=true
        }
        if (path.length >= 1) { // If there are only 1 parts or less, the file modified doesn't belong to a project
            for (i = 0; i < path.length; i++) {
                if (i + 1 <= path.length) { // not the last item
                    applications.push(path[i+1])
                }
            }
        }
        ​
    }
    return applications.unique()
}
def getBuildFilePath(String component) {
    return component + './build.yaml'
}
​
def replaceSensitiveBuildArgsValues(buildArgs, Map svmVars) {
    def newBuildArgs = []
    buildArgs.each { map ->
        def entries = map.entrySet()
        entries.each { entry ->
            if (entry.key == "GITHUB_TOKEN") {
                withCredentials([string(credentialsId: 'github-token', variable: 'GITHUB_TOKEN')]) {
                    echo "GITHUB_TOKEN replaced"
                    newBuildArgs.add(["GITHUB_TOKEN": env.GITHUB_TOKEN])
                }
            } else if (entry.key == "REPOSITORY_BRANCH") {
                branch = getBranchName(svmVars)
                echo "REPOSITORY_BRANCH replaced to "+
                        newBuildArgs.add(["REPOSITORY_BRANCH": getBranchName(svmVars)])
            } else {
                newBuildArgs.add(map)
            }
        }
    }
    return newBuildArgs
}
​
def getBranchName(Map scm) {
    def branchName = ""
    ​
    if(scm.GIT_BRANCH.contains('issue/') || scm.GIT_BRANCH.contains('release/')) {
        branchName = scm.GIT_BRANCH.split('/', 2).join("/") // Split first occurrence
    }
    ​
    if(scm.GIT_BRANCH.contains('PR-')) {
        branchName = env.CHANGE_BRANCH.split('/', 2).join("/")
    }
    if(scm.GIT_BRANCH.contains('master')) {
        branchName = "master"
    }
    if (branchName == "")
        throw new Exception("Cannot infer the branchName for: "+scm.GIT_BRANCH)
    ​
    return branchName
}
​
def getModules(String component) {
    def buildPath = getBuildFilePath(component)
    def data = readYaml(file: buildPath)
    def modules = []
    ​
    if(data.modules == null || data.modules.size() < 1) {
        return []
    }
    ​
    for(LinkedHashMap item : data.modules) {
        def moduleEntries = item.entrySet()
        ​
        moduleEntries.each { moduleEntry ->
            modules.push(moduleEntry.key)
        }
    }
    return modules
}
​
def getDefaultBuildArgs(String component, Map scmVars) {
    def buildPath = getBuildFilePath(component)
    ​
    def data = readYaml(file: buildPath)
    def buildArgs = ""
    ​
    replaceSensitiveBuildArgsValues(data.build_args, scmVars).each { map ->
        def entries = map.entrySet()
        entries.each { entry ->
            buildArgs = buildArgs + "--build-arg ${entry.key}=${entry.value} "
        }
    }
    return buildArgs
}
​
def getDefaultTagVersion(buildPath) {
    def data = readYaml(file: buildPath)
    if (data.tag_version == null) {
        throw new Exception('Tag version not defined')
    }
    return data.tag_version
}
​
def executeGitCommandWithCredentials(String command){
    String stdOut
    withCredentials([string(credentialsId: 'github-token', variable: 'GITHUB_TOKEN')]){
        stdOut =  sh(script: """
               git remote rm origin
               git remote add origin https://${env.GITHUB_TOKEN}@github.com/globeandmail/snowplow-stream-loader.git
               git config --global user.email "${env.GITHUB_TOKEN}@sophi.com"
               git config --global user.name "${env.GITHUB_TOKEN}"
               ${command}
""",returnStdout: true)
        ​
    }
    return stdOut
    ​
}
​
def incrementVersionAndCommit(String currentTag, String changeType, String branchName,String buildPath,Boolean gitPush,String moduleName=""){
    //Get tag from master branch
    def developTag = getDevelopTag(buildPath,moduleName)
    def newTag = ""
    if(developTag == currentTag){
        newTag = bumpTag(currentTag,changeType)
        ​
    }
    if(developTag > currentTag){
        newTag = bumpTag(developTag.toString(),changeType)
    }
    ​
    if(developTag < currentTag){
        echo "Current tag is already updated to $currentTag"
        newTag = currentTag
        ​
    }
    ​
    if(developTag >= currentTag){
        updateBuildFileAndCommit(newTag,buildPath,branchName,gitPush,moduleName)
        isGitPush.add(true)
    }
    else{
        echo 'Version already updated'
        isGitPush.add(false)
    }
    ​
    echo "New tag value is ${newTag}"
    ​
    return newTag
    ​
}
​
def getLabel(String branchName) {
    def labels
    try{
        labels = getLabelFromPrApi(branchName)
    }
    catch (e){
        echo "No Version Labels found, failing build"
        notifyFailed(e)
    }
    ​
    return labels
}
​
def commitTagToGit(String tag,String moduleName="") {
    def commitTag
    if(!moduleName.isEmpty()){
        moduleName = moduleName+'/'
    }
    commitTag = moduleName.concat(tag)
    echo "$commitTag is the new release."
    echo "Executing: git tag -a $commitTag -m 'Tag release $commitTag'"
    executeGitCommandWithCredentials("""git fetch -q
                        git tag -a $commitTag -m 'Tag release $commitTag'
                        git push origin --tags
                        echo "Pushed new tag $commitTag successfully"
""")
}
​
def getImageNamesAndBuildArgs(component, scmVars) {
    def buildPath = getBuildFilePath( component)
    ​
    def baseImageName = "harbor.sophi.io/sophi4/"
    def branchName = getBranchName(scmVars)
    def modules
    def imageName
    def imageNamesAndBuildArgs = []
    def imageTag
    def labels = ""
    ​
    ​
    modules = getModules(component)
    if (modules == null || modules.isEmpty()) {
        // regular build with no modules
        def buildArgs = getDefaultBuildArgs(component, scmVars)
        if(branchName.contains("master")){
            imageTag = getDefaultTagVersion(buildPath)
            commitTagToGit(imageTag)
        }
        else if(scmVars.GIT_BRANCH.contains("PR-")){
            labels = getLabel(scmVars.GIT_BRANCH.toString())
            imageTag = getDefaultTagVersion(buildPath)
            //If build.yaml is changed manually
            if(!isManualMode){
                imageTag = incrementVersionAndCommit(imageTag.toString(),labels.toString(),branchName,buildPath,true)
            }
            ​
        }
        else {
            imageTag = getDefaultTagVersion(buildPath) +"-" + branchName.replace("issue/","")
        }
        imageName = baseImageName + component
        imageNamesAndBuildArgs.push([imageName: imageName, buildArgs: buildArgs, push: shallPush(buildPath),
                                     imageTag: imageTag.toString().toLowerCase(), prebuildCommands: getPrebuildCommands(buildPath)])
    } else {
        // modular build with multiple images
        sh("""echo "modular build with multiple images" """)
        def imageTags = []
        modules.each { moduleName ->
            def moduleSettings = []
            ​
            moduleSettings = getModuleSettings(category, component, moduleName, scmVars,branchName,false)
            buildPath = getBuildFilePath(component)
            if(getBranchName(scmVars).contains("master")){
                imageTag = moduleSettings.tagVersion.join("")
                //Create list of all tags
                imageTags.push(imageTag)
            }
            else if(scmVars.GIT_BRANCH.contains("PR-")){
                moduleSettings = getModuleSettings(category, component, moduleName, scmVars,branchName,true)
                //First do all module commits and git push once
                if(modules.indexOf(moduleName)==modules.size()-1 && !isManualMode){
                    echo "Inside gitPush"
                    def isTagUpdateBuild = moduleSettings
                    gitPush(branchName)
                }
                ​
            }
            else{
                imageTag = (moduleSettings.tagVersion +"-" + branchName.replace("issue/","")).join("")
            }
            imageName = baseImageName + component  + "_" + moduleName
            imageNamesAndBuildArgs.push([imageName: imageName, buildArgs: moduleSettings.buildArgs.join(" "),
                                         push: moduleSettings.push[0], imageTag: imageTag.toString().toLowerCase(), prebuildCommands: [] ])
        }
        ​
        //Take the highest tag version amongst all the modules as git release version
        if(imageTags.size() > 0){
            def maxTag = imageTags.max()
            commitTagToGit(maxTag.toString(),modules.join("-"))
        }
    }
    ​
    return imageNamesAndBuildArgs
}
​
​podTemplate(label: label, containers: [
        containerTemplate(name: 'docker', image: 'docker:stable-git', command: 'cat', ttyEnabled: true, resourceRequestCpu: '500m', resourceLimitCpu: '1000m',
                resourceRequestMemory: '512Mi', resourceLimitMemory: '2500Mi',
                envVars: [
                        envVar(key: 'TAG_UPDATE_BUILD', value: 'false')
                ])
//  , containerTemplate(name: 'debian', image: 'debian', command: 'cat', ttyEnabled: true)
],
        volumes: [
                hostPathVolume(mountPath: '/var/run/docker.sock', hostPath: '/var/run/docker.sock')
        ]) {
    node(label) {
        properties([disableConcurrentBuilds()])
        def scmVars
        def uniqueApplications
        def imageNamesAndBuildArgs


        //notifyStarted()

        ​
        stage('Checkout Repo') {
            try {
                scmVars = checkoutCurrentRepo()
            } catch (e) {
                notifyFailed(e)
            }
        }
        ​
        stage('Extract Apps') {
            try {
                env.GIT_COMMIT = scmVars.GIT_COMMIT
                echo "COMMIT SHA IS : ${scmVars.GIT_COMMIT}"
                def filesChanged
                if (scmVars.GIT_BRANCH.containes("PR")) {
                    // for PR, Jenkins merges the branch to double-check. So we have to go one commit back.
                    filesChanged = getFileChangesFromApi(env.CHANGE_ID)
                } else {
                    filesChanged = sh(script: "git diff-tree --no-commit-id --name-only -r \${GIT_COMMIT} ", returnStdout: true)
                }
                uniqueApplications = extractAppsToDeploy(filesChanged)
            } catch (e) {
                notifyFailed(e)
            }
        }
        ​
        stage('Build Image') {
            uniqueApplications.each { component ->
                def buildPath = getBuildFilePath(component)
                if (!fileExists(buildPath)) {
                    echo "No 'build.yaml' file under" + buildPath
                } else {
                    echo "building: " + component
                    container('docker') {
                        withCredentials([[$class          : 'UsernamePasswordMultiBinding',
                                          credentialsId   : 'harbor-account',
                                          usernameVariable: 'HARBOR_ACCOUNT_USER',
                                          passwordVariable: 'HARBOR_ACCOUNT_PASSWORD']]) {
                            wrap([$class: 'AnsiColorBuildWrapper', 'colorMapName': 'XTerm']) {
                                sh("docker login -u ${HARBOR_ACCOUNT_USER} -p ${HARBOR_ACCOUNT_PASSWORD} harbor.sophi.io")
                                imageNamesAndBuildArgs = getImageNamesAndBuildArgs(category, component, scmVars)
                                imageNamesAndBuildArgs.each { item ->
                                    imageName = item.imageName
                                    buildArgs = item.buildArgs
                                    shallPushImage = item.push
                                    imageTag = item.imageTag
                                    prebuildCommands = item.prebuildCommands
                                    ​
                                    prebuildCommands.each {
                                        command ->
                                            print sh(script: "${command}", returnStdout: true)
                                    }
                                    sh("docker build --network host ${buildArgs} -t ${imageName}:${imageTag} ${getDockerFilePath(category, component)}")
                                    if (shallPushImage) {
                                        sh("docker push ${imageName}")
                                    }
                                }
                            }
                        }
                    }
                }
                ​
            }
        }
        notifySuccess()
    }
}