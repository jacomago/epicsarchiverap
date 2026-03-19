plugins {
	`java-library`
	war
	alias(libs.plugins.analyze)
}

dependencies {
	implementation(project(":shared"))
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

tasks.register<War>("etlWar") {
	group = "Wars"
	description = "Builds etl.war."
	archiveFileName.set("etl.war")
}
