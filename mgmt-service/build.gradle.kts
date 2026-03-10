import java.io.FileOutputStream

plugins {
	`java-library`
	war
	alias(libs.plugins.analyze)
}

dependencies {
	implementation(project(":shared"))
	implementation(libs.tomcat.servlet.api)
}

val apiDocsDir = layout.projectDirectory.dir("docs/api")

tasks.withType<Javadoc>().configureEach {
	dependsOn("generateBPLActionsMappings")
	mustRunAfter("generateBPLActionsMappings")
	finalizedBy("generateJavaDocTagletScriptables")
	source = sourceSets.main.get().allJava
	options {
		require(this is StandardJavadocDocletOptions)
		author(true); version(true); isUse = true
		links("https://docs.oracle.com/en/java/javase/21/docs/api/")
		taglets(
			"org.epics.archiverappliance.taglets.BPLActionTaglet",
			"org.epics.archiverappliance.taglets.BPLActionParamTaglet",
			"org.epics.archiverappliance.taglets.BPLActionEndTaglet"
		)
		tagletPath(tagletConfig.files.toList())
	}
}

tasks.register<JavaExec>("generateBPLActionsMappings") {
	group = "Documentation"
	outputs.file("${apiDocsDir}/mgmtpathmappings.txt")
	doFirst {
		apiDocsDir.asFile.mkdirs()
		standardOutput = FileOutputStream("${apiDocsDir}/mgmtpathmappings.txt")
	}
	mainClass.set("org.epics.archiverappliance.mgmt.BPLServlet")
	classpath = sourceSets.main.get().runtimeClasspath
}

tasks.register<JavaExec>("generateJavaDocTagletScriptables") {
	group = "Documentation"
	dependsOn("generateBPLActionsMappings")
	mainClass.set("org.epics.archiverappliance.common.taglets.ProcessMgmtScriptables")
	inputs.file("${apiDocsDir}/mgmt_scriptables.txt")
	outputs.file("${apiDocsDir}/mgmt_scriptables.html")
	classpath = sourceSets.main.get().runtimeClasspath
	workingDir = project.projectDir
}

tasks.register<Exec>("sphinx") {
	group = "Staging"
	description = "Generate the documentation site."
	dependsOn(tasks.javadoc)
	workingDir = rootProject.file("docs")
	outputs.dir("${workingDir}/docs/build")
	commandLine(
		if (org.apache.tools.ant.taskdefs.condition.Os.isFamily(
				org.apache.tools.ant.taskdefs.condition.Os.FAMILY_WINDOWS))
			listOf("cmd", "/c", "build_docs.bat") else listOf("./build_docs.sh")
	)
}

tasks.register<War>("mgmtWar") {
	group = "Wars"
	description = "Builds mgmt.war."
	dependsOn("sphinx")
	val stageDir: Directory by rootProject.extra
	from(stageDir.asFile.resolve("org/epics/archiverappliance/mgmt/staticcontent")) {
		into("ui")
	}
	from(project(":shared").projectDir.resolve("src/main/java/org/epics/archiverappliance/config/persistence")) {
		include("*.sql"); into("install")
	}
	from(rootProject.file("docs/docs/build")) { into("ui/help") }
	from(rootProject.file("docs/docs/source/samples")) {
		include("deployMultipleTomcats.py"); into("install")
	}
	from(project(":shared").projectDir.resolve("src/main/java/org/epics/archiverappliance/common/scripts")) {
		filePermissions {
			user { read = true; write = true; execute = true }
			group { read = true; execute = true }
			other { read = true; execute = true }
		}
		into("install/pbutils")
	}
	archiveFileName.set("mgmt.war")
}
