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

// Test tasks added in Phase 3
