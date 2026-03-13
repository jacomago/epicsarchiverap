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

tasks.register<Test>("integrationTests") {
	group = "Verification"
	description = "Runs integration tests against embedded Tomcat. ARCHAPPL_SITEID=tests required."
	useJUnitPlatform { includeTags("integration"); excludeTags("slow", "flaky") }
	forkEvery = 1
	maxParallelForks = 1
	dependsOn(
		":mgmt-service:mgmtWar",
		":retrieval-service:retrievalWar",
		":etl-service:etlWar",
		":engine-service:engineWar",
	)
	doFirst {
		systemProperty("mgmt.war",      project(":mgmt-service").tasks.named<War>("mgmtWar").get().archiveFile.get().asFile.absolutePath)
		systemProperty("retrieval.war", project(":retrieval-service").tasks.named<War>("retrievalWar").get().archiveFile.get().asFile.absolutePath)
		systemProperty("etl.war",       project(":etl-service").tasks.named<War>("etlWar").get().archiveFile.get().asFile.absolutePath)
		systemProperty("engine.war",    project(":engine-service").tasks.named<War>("engineWar").get().archiveFile.get().asFile.absolutePath)
	}
}
