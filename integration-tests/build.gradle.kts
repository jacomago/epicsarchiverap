plugins {
	`java-library`
}

dependencies {
	testImplementation(project(":shared"))
	testImplementation(project(":engine-service"))
	testImplementation(project(":retrieval-service"))
	testImplementation(project(":etl-service"))
	testImplementation(project(":mgmt-service"))
	testImplementation(libs.bundles.junit.impl)
	testRuntimeOnly(libs.junit.jupiter.engine)
	testRuntimeOnly(libs.junit.platform.launcher)
	testImplementation(libs.awaitility)
	testImplementation(libs.mockito)
	testImplementation(libs.jinjava)
	testImplementation(libs.tomcat.servlet.api)
	testImplementation(project(":taglets"))
}

tasks.named<ProcessResources>("processTestResources") {
	from(layout.projectDirectory.dir("src/test/resources/classpathfiles"))
	from(layout.projectDirectory.dir("src/test/resources")) {
		include("log4j2.xml", "appliances.xml.j2", "log4j2.component.properties")
	}
}

tasks.register<Exec>("shutdownAllTomcats") {
	group = "Verification"
	description = "Kill any leaked Tomcat processes from integration tests."
	isIgnoreExitValue = true
	if (!org.apache.tools.ant.taskdefs.condition.Os.isFamily(
			org.apache.tools.ant.taskdefs.condition.Os.FAMILY_WINDOWS)) {
		commandLine("pkill", "-9", "-f", "Deaatag=eaatesttm")
	}
}

tasks.register<Test>("integrationTests") {
	group = "Verification"
	description = "Runs integration tests against embedded Tomcat. ARCHAPPL_SITEID=tests required."
	useJUnitPlatform { includeTags("integration"); excludeTags("slow", "flaky") }
	forkEvery = 1
	maxParallelForks = 1
	finalizedBy("shutdownAllTomcats")
	environment("ARCHAPPL_SHORT_TERM_FOLDER", temporaryDir.resolve("sts").path)
	environment("ARCHAPPL_MEDIUM_TERM_FOLDER", temporaryDir.resolve("mts").path)
	environment("ARCHAPPL_LONG_TERM_FOLDER", temporaryDir.resolve("lts").path)
	dependsOn(
		":mgmt-service:mgmtWar",
		":retrieval-service:retrievalWar",
		":etl-service:etlWar",
		":engine-service:engineWar",
	)
	doFirst {
		temporaryDir.resolve("sts").mkdirs()
		temporaryDir.resolve("mts").mkdirs()
		temporaryDir.resolve("lts").mkdirs()
		systemProperty("mgmt.war",      project(":mgmt-service").tasks.named<War>("mgmtWar").get().archiveFile.get().asFile.absolutePath)
		systemProperty("retrieval.war", project(":retrieval-service").tasks.named<War>("retrievalWar").get().archiveFile.get().asFile.absolutePath)
		systemProperty("etl.war",       project(":etl-service").tasks.named<War>("etlWar").get().archiveFile.get().asFile.absolutePath)
		systemProperty("engine.war",    project(":engine-service").tasks.named<War>("engineWar").get().archiveFile.get().asFile.absolutePath)
	}
	doLast {
		delete(temporaryDir.resolve("sts"), temporaryDir.resolve("mts"), temporaryDir.resolve("lts"))
	}
}

tasks.register<Test>("automationTests") {
	group = "Verification"
	description = "Run all tests (no tag filter) in a CI environment."
	forkEvery = 1
	maxParallelForks = 1
	finalizedBy("shutdownAllTomcats")
}

tasks.register<JavaExec>("testRun") {
	group = "Verification"
	description = "Start the appliance for manual testing via embedded Tomcat."
	environment("ARCHAPPL_SHORT_TERM_FOLDER", "${rootProject.layout.buildDirectory.get()}/storage/sts")
	environment("ARCHAPPL_MEDIUM_TERM_FOLDER", "${rootProject.layout.buildDirectory.get()}/storage/mts")
	environment("ARCHAPPL_LONG_TERM_FOLDER", "${rootProject.layout.buildDirectory.get()}/storage/lts")
	mainClass.set("org.epics.archiverappliance.TestRun")
	classpath = sourceSets.test.get().runtimeClasspath
	args("-c2")
}
