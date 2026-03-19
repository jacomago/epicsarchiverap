import java.io.FileOutputStream

plugins {
	`java-library`
	war
	alias(libs.plugins.analyze)
}

dependencies {
	implementation(project(":shared"))
	implementation(project(":shared-eventstream"))
	implementation(libs.tomcat.servlet.api)

	// Testing
	testImplementation(libs.junit.jupiter.api)
	testRuntimeOnly(libs.junit.jupiter.engine)
	testRuntimeOnly(libs.junit.platform.launcher)
	testImplementation(libs.mockito)
}

tasks.withType<Test>().configureEach {
	useJUnitPlatform()
}

val tagletConfig: Configuration by configurations.creating

dependencies {
	tagletConfig(project(":taglets"))
}

val apiDocsDir = rootProject.layout.projectDirectory.dir("docs/api")
val mgmtScriptablesTxt = layout.buildDirectory.file("tmp/mgmt_scriptables.txt")
val mgmtPathMappingsTxt = layout.buildDirectory.file("tmp/mgmtpathmappings.txt")
val mgmtScriptablesHtml = apiDocsDir.file("mgmt_scriptables.html")

tasks.withType<Javadoc>().configureEach {
	dependsOn("generateBPLActionsMappings", ":taglets:jar")
	mustRunAfter("generateBPLActionsMappings")
	finalizedBy("generateJavaDocTagletScriptables")
	source = sourceSets.main.get().allJava
	doFirst {
		mgmtPathMappingsTxt.get().asFile.parentFile.mkdirs()
		// Gradle writes all javadoc options (including -J flags) to an @options file,
		// but javadoc ignores -J flags in @files. The javadoc process CWD is the
		// subproject directory (mgmt-service/), so write the config to build/tmp/
		// which the taglet finds at its default path relative to CWD.
		mgmtPathMappingsTxt.get().asFile.parentFile.resolve("taglets.properties").writeText(
			"pathMappings=${mgmtPathMappingsTxt.get().asFile.absolutePath}\n" +
			"scriptablesFile=${mgmtScriptablesTxt.get().asFile.absolutePath}\n"
		)
	}
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
	outputs.file(mgmtScriptablesTxt)
}

tasks.register<JavaExec>("generateBPLActionsMappings") {
	group = "Documentation"
	outputs.file(mgmtPathMappingsTxt)
	doFirst {
		apiDocsDir.asFile.mkdirs()
		mgmtPathMappingsTxt.get().asFile.parentFile.mkdirs()
		standardOutput = FileOutputStream(mgmtPathMappingsTxt.get().asFile)
	}
	mainClass.set("org.epics.archiverappliance.mgmt.BPLServlet")
	classpath = sourceSets.main.get().runtimeClasspath
}

tasks.register<JavaExec>("generateJavaDocTagletScriptables") {
	group = "Documentation"
	dependsOn("generateBPLActionsMappings", "javadoc")
	mainClass.set("org.epics.archiverappliance.common.taglets.ProcessMgmtScriptables")
	inputs.file(mgmtScriptablesTxt)
	doFirst {
		standardOutput = FileOutputStream("${apiDocsDir}/mgmt_scriptables.html")
    }
	outputs.file("${apiDocsDir}/mgmt_scriptables.html")
	args(mgmtScriptablesTxt.get().asFile.absolutePath)
	args(mgmtPathMappingsTxt.get().asFile.absolutePath)
	classpath = sourceSets.main.get().runtimeClasspath
	workingDir = rootProject.projectDir
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
