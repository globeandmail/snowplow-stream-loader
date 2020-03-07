def label = "worker-${UUID.randomUUID().toString()}"
isGitPush = []
isManualMode = false
def notifyStarted() {
    slackSend(
            color: 'warning',
            message: "STARTED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})",
    )
}

def notifySuccess() {
    slackSend(
            color: 'good',
            message: "SUCCESS: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL})",
    )
}

def notifyFailed(Exception e) {
    slackSend(
            color: 'danger',
            message: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]' (${env.BUILD_URL}): ${e.toString()}"
    )

    throw e
}

def checkoutCurrentRepo() {
    def vars = checkout scm

    return vars
}

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
        echo "-------------- ${path.length} -------------"
        /*if (path.length >= 1) { // If there are only 1 parts or less, the file modified doesn't belong to a project
            for (i = 0; i < path.length; i++) {
                applications.push(path[i + 1])
                echo "${path[i + 1]}"
            }
        }*/

    }

    return applications.unique()
}

def getBuildFilePath(String component) {
    return component + './build.yaml'
}


def getDefaultTagVersion(buildPath) {
    def data = readYaml(file: buildPath)
    if (data.tag_version == null) {
        throw new Exception('Tag version not defined')
    }
    return data.tag_version
}

def getPrebuildCommands(buildPath) {
    def data = readYaml(file: buildPath)
    if (data.prebuild_commands == null) {
        return []
    } else {
        return data.prebuild_commands
    }
}

def getModules(String component) {
    def buildPath = getBuildFilePath(component)
    def data = readYaml(file: buildPath)
    def modules = []

    if (data.modules == null || data.modules.size() < 1) {
        return []
        //throw new Exception('No modules found. Declare at least one module.')
    }

    for (LinkedHashMap item : data.modules) {
        def modulesEntries = item.entrySet()

        modulesEntries.each { moduleEntry ->
            modules.push(moduleEntry.key)
        }
    }

    return modules
}

def getVersion() {
    def fullPath = 'VERSION'

    if(fileExists(fullPath)) {
        def fileContent = readFile(fullPath)

        return "_" + fileContent.trim()
    }

    return ""
}

def getBranchName(Map scm) {
    def branchName = ""

    if(scm.GIT_BRANCH.contains('issue/') || scm.GIT_BRANCH.contains('release/')) {
        branchName = scm.GIT_BRANCH.split('/', 2).join("/") // Split first occurrence
    }

    if(scm.GIT_BRANCH.contains('PR-')) {
        branchName = env.CHANGE_BRANCH.split('/', 2).join("/")
    }
    if(scm.GIT_BRANCH.contains('master')) {
        branchName = "master"
    }
    if (branchName == "")
        throw new Exception("Cannot infer the branchName for: "+scm.GIT_BRANCH)

    return branchName
}

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

def getFileChangesFromApi(String prId) {
    def jsonResp
    def fileChanges
    def fileNames

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

def getLabelFromPrApi(String branchName) {
    def label
    def jsonResp
    String labelName
    withCredentials([string(credentialsId: 'github-token', variable: 'GITHUB_TOKEN')]) {
        jsonResp = sh(
                script: "wget --header 'Authorization: Bearer ${env.GITHUB_TOKEN}' -qO- https://api.github.com/repos/globeandmail/snowplow-stream-loader/pulls/${env.CHANGE_ID}",
                returnStdout: true
        )
        def prInfo = readJSON(text: jsonResp)
        label = prInfo.labels.find { ['major','minor','patch'].contains(it.name.toString().toLowerCase()) }
        labelName = label.name
        echo "PR label is ${labelName}"

    }

    return labelName.toLowerCase()
}

def getLabelFromCommitsApi(String commitSha) {
    def label
    String labelName
    def jsonResp

    withCredentials([string(credentialsId: 'github-token', variable: 'GITHUB_TOKEN')]) {
        jsonResp = sh(
                script: "wget --header 'Authorization: Bearer ${env.GITHUB_TOKEN}' " +
                        "--header 'Accept: application/vnd.github.groot-preview+json' " +
                        "-qO- https://api.github.com/repos/globeandmail/snowplow-stream-loader/commits/${commitSha}/pulls",
                returnStdout: true
        )
        def prInfo = readJSON(text: jsonResp)

        label = prInfo.labels.find { ['major','minor','patch'].contains(it.name.head().toString().toLowerCase()) }
        labelName = label.name.head().toString().toLowerCase()
    }
    echo "label is ${labelName}"

    return labelName

}

def getLabel(String branchName) {
    def labels
    try{
        labels = getLabelFromPrApi(branchName)
    }
    catch (e){
        echo "No Version Labels found, failing build"
        notifyFailed(e)
    }

    return labels
}

def getModuleSettings(String component, String module, Map scmVars,String branchName,Boolean isPR) {
    def buildPath = getBuildFilePath(component)

    def data = readYaml(file: buildPath)
    def buildArgs = ""
    def moduleSettings = []

    for (LinkedHashMap item : data.modules) {
        def modulesEntries = item.entrySet()

        modulesEntries.each { moduleEntry ->
            if (moduleEntry.key == module) {
                buildArgs = ""
                def newTag = ""
                def build_args = moduleEntry.value.build_args
                replaceSensitiveBuildArgsValues(build_args, scmVars).each { map ->
                    def entries = map.entrySet()
                    entries.each { entry ->
                        buildArgs = buildArgs + "--build-arg ${entry.key}=${entry.value} "
                    }
                }

                if (moduleEntry.value.tag_version == null) {
                    throw new Exception("Tag version not defined for module " + moduleEntry.key)
                }
                else{
                    if(isPR){
                        def tagVersion = moduleEntry.value.tag_version
                        if(!isManualMode){
                            def changeType = getLabel(branchName)
                            newTag = incrementVersionAndCommit(tagVersion.toString(),
                                    changeType.toString(), branchName,buildPath,false,module
                            )
                        }
                        else{
                            newTag=tagVersion
                        }

                    }
                    else{
                        newTag = moduleEntry.value.tag_version
                    }

                }

                moduleSettings.push(['buildArgs':buildArgs, tagVersion: newTag, push: moduleEntry.value.push==null || moduleEntry.value.push ==true])
            }
        }
    }

    return moduleSettings
}


def getDefaultBuildArgs(String component, Map scmVars) {
    def buildPath = getBuildFilePath(component)

    def data = readYaml(file: buildPath)
    def buildArgs = ""

    replaceSensitiveBuildArgsValues(data.build_args, scmVars).each { map ->
        def entries = map.entrySet()
        entries.each { entry ->
            buildArgs = buildArgs + "--build-arg ${entry.key}=${entry.value} "
        }
    }
    return buildArgs
}
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

    }
    return stdOut

}

def getLatestTagFromGit(){
    String currentTag = executeGitCommandWithCredentials("""git fetch -q 
                          git describe --tags \$(git rev-list --tags --max-count=1)""")
    currentTagArray = currentTag.split("/")
    if(currentTagArray.size()>1){
        currentTag=currentTagArray[1]
    }
    return currentTag
}

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


def getModuleByName(Object data,String name){
    LinkedHashMap module
    for ( LinkedHashMap item in data.modules) {
        def modulesEntries = item.entrySet()
        def entry = modulesEntries.find{it.key==name}
        if(entry!=null){
            module = item
            break
        }


    }
    return module

}
def getModuleTagVersion(String tempFile,String moduleName){
    def data = readYaml(file: tempFile)
    def tagVersion=""
    LinkedHashMap module = getModuleByName(data,moduleName)
    tagVersion = module.entrySet().head().value.tag_version
    return tagVersion


}
def getDevelopTag(String buildPath, String moduleName=""){
    def mt
    def tempFile = 'tagVersion.yaml'
    executeGitCommandWithCredentials("""
    git fetch -q
    git show origin/develop:$buildPath > $tempFile""")
    //if single module build
    if(moduleName.isEmpty()){
        mt = getDefaultTagVersion(tempFile)
        echo "Develop tag is $mt"
    }
    else{
        //if multiple modules built
        mt = getModuleTagVersion(tempFile,moduleName)
        echo "Develop tag for module $moduleName is $mt"
    }


    return mt

}

/**
 *
 * @param tag The current tag version
 * @param changeType The label on the PR. Valid values are 'major','minor' or 'patch'
 * @return
 */
def bumpTag(String tag, String changeType){
    def elements = tag.split("\\.")
    def newTag
    def major,minor,patch
    if (elements.length == 3) {
        major = elements[0]
        minor = elements[1]
        patch = elements[2]

        switch (changeType) {
            case 'major':
                minor = '0'
                patch = '0'
                newTag = (major.toInteger() +1).toString()+"."+minor+"."+patch
                break

            case 'minor':
                patch = '0'
                newTag = major+"."+(minor.toInteger() +1).toString()+"."+patch
                break
            case 'patch':
                newTag = major+"."+minor+"."+(patch.toInteger() + 1).toString()
                break
            default:
                throw new Exception("Invalid Label type ${changeType}")
                break

        }
        return newTag
    }
}


def updateBuildFileAndCommit(String tag, String buildPath,String branchName,Boolean gitPush,String moduleName=""){
    //need to reinitiate git credentials
    executeGitCommandWithCredentials("""
                        git fetch -q
                        git checkout $branchName""".trim())
    if(moduleName.toString().empty){
        def data = readYaml(file: buildPath)
        data.tag_version = tag
        sh("rm $buildPath")
        writeYaml(file: buildPath, data: data)
    }
    else{
        //If multiple modules need to rewrite yaml after every update
        Object data = readYaml(file: buildPath)
        LinkedHashMap module = getModuleByName(data,moduleName)
        module.entrySet().head().value.tag_version = tag
        def oldModuleIndex = data.modules.indexOf(getModuleByName(data,moduleName))
        data.modules[oldModuleIndex] = module
        sh("rm $buildPath")
        writeYaml(file: buildPath, data: data)
    }

    //if git push is enabled for single commit
    if(gitPush){
        executeGitCommandWithCredentials("""
                     git commit $buildPath -m 'Updated $buildPath tag version to $tag'
                     git push --set-upstream origin $branchName """)
    }
    else{
        executeGitCommandWithCredentials("git commit $buildPath -m 'Updated $buildPath tag version to $tag'")
    }


    echo "Updated file $buildPath,commiting"


}

def incrementVersionAndCommit(String currentTag, String changeType, String branchName,String buildPath,Boolean gitPush,String moduleName=""){
    //Get tag from master branch
    def developTag = getDevelopTag(buildPath,moduleName)
    def newTag = ""
    if(developTag == currentTag){
        newTag = bumpTag(currentTag,changeType)

    }
    if(developTag > currentTag){
        newTag = bumpTag(developTag.toString(),changeType)
    }

    if(developTag < currentTag){
        echo "Current tag is already updated to $currentTag"
        newTag = currentTag

    }

    if(developTag >= currentTag){
        updateBuildFileAndCommit(newTag,buildPath,branchName,gitPush,moduleName)
        isGitPush.add(true)
    }
    else{
        echo 'Version already updated'
        isGitPush.add(false)
    }

    echo "New tag value is ${newTag}"

    return newTag

}

def gitPush(String branchName){
    if(isGitPush.contains(true)){
        executeGitCommandWithCredentials("""
                     git fetch -q 
                     git push --set-upstream origin $branchName""")
    }
    else{
        echo "Version already updated, skipping git push"
    }

}


def getImageNamesAndBuildArgs(component, scmVars) {
    def buildPath = getBuildFilePath(component)

    def baseImageName = "harbor.sophi.io/sophi4/"
    def branchName = getBranchName(scmVars)
    def modules
    def imageName
    def imageNamesAndBuildArgs = []
    def imageTag
    def labels = ""


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

            moduleSettings = getModuleSettings(component, moduleName, scmVars,branchName,false)
            buildPath = getBuildFilePath(component)
            if(getBranchName(scmVars).contains("master")){
                imageTag = moduleSettings.tagVersion.join("")
                //Create list of all tags
                imageTags.push(imageTag)
            }
            else if(scmVars.GIT_BRANCH.contains("PR-")){
                moduleSettings = getModuleSettings(component, moduleName, scmVars,branchName,true)
                //First do all module commits and git push once
                if(modules.indexOf(moduleName)==modules.size()-1 && !isManualMode){
                    echo "Inside gitPush"
                    def isTagUpdateBuild = moduleSettings
                    gitPush(branchName)
                }

            }
            else{
                imageTag = (moduleSettings.tagVersion +"-" + branchName.replace("issue/","")).join("")
            }
            imageName = baseImageName + component  + "_" + moduleName
            imageNamesAndBuildArgs.push([imageName: imageName, buildArgs: moduleSettings.buildArgs.join(" "),
                                         push: moduleSettings.push[0], imageTag: imageTag.toString().toLowerCase(), prebuildCommands: [] ])
        }

        //Take the highest tag version amongst all the modules as git release version
        if(imageTags.size() > 0){
            def maxTag = imageTags.max()
            commitTagToGit(maxTag.toString(),modules.join("-"))
        }
    }

    return imageNamesAndBuildArgs
}

def getChartParameters(component, module) {
    def buildPath = getBuildFilePath(component)
    def parameters = ""

    if (fileExists(buildPath)) {
        def data = readYaml(file: buildPath)

        for (LinkedHashMap item : data.modules) {
            def modulesEntries = item.entrySet()

            modulesEntries.each { moduleEntry ->
                if (moduleEntry.key == module) {
                    def envVars = moduleEntry.value.env

                    envVars.each { k, v ->
                        parameters = parameters + "--set env.${k}=${v} "
                    }
                }
            }
        }
    }

    return parameters
}

def shallPush(String buildPath) {
    def data = readYaml(file: buildPath)
    return data.push==null || data.push ==true
}

podTemplate(label: label, containers: [
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



        stage('Checkout Repo') {
            try {
                scmVars = checkoutCurrentRepo()
            } catch (e) {
                notifyFailed(e)
            }
        }

        stage('Extract Apps') {
            try {
                env.GIT_COMMIT = scmVars.GIT_COMMIT
                echo "COMMIT SHA IS : ${scmVars.GIT_COMMIT}"
                def filesChanged
                if (scmVars.GIT_BRANCH.contains("PR")){
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

        stage('Build image') {
            uniqueApplications.each { component ->
                try {
                    def buildPath = getBuildFilePath(component)
                    if (!fileExists(buildPath)) {
                        echo "No 'build.yaml; file under " + buildPath
                    } else {
                        echo "building: " + component
                        container('docker') {
                            withCredentials([[$class          : 'UsernamePasswordMultiBinding',
                                              credentialsId   : 'harbor-account',
                                              usernameVariable: 'HARBOR_ACCOUNT_USER',
                                              passwordVariable: 'HARBOR_ACCOUNT_PASSWORD']]) {
                                wrap([$class: 'AnsiColorBuildWrapper', 'colorMapName': 'XTerm']) {

                                    sh("docker login -u ${HARBOR_ACCOUNT_USER} -p ${HARBOR_ACCOUNT_PASSWORD} harbor.sophi.io")
                                    imageNamesAndBuildArgs = getImageNamesAndBuildArgs(component, scmVars)
                                    imageNamesAndBuildArgs.each { item ->
                                        imageName = item.imageName
                                        buildArgs = item.buildArgs
                                        shallPushImage = item.push
                                        imageTag = item.imageTag
                                        prebuildCommands = item.prebuildCommands

                                        prebuildCommands.each {
                                            command ->
                                                print sh(script: "${command}", returnStdout: true)
                                        }
                                        sh("docker build --network host ${buildArgs} -t ${imageName}:${imageTag} ${getDockerFilePath(component)}")
                                        if (shallPushImage) {
                                            sh("docker push ${imageName}")
                                        }
                                    }
                                }
                            }
                        }
                    }

                } catch (e) {
                    notifyFailed(e)
                }
            }

        }

        notifySuccess()
    }
}