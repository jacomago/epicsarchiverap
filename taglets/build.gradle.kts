plugins {
	`java-library`
	alias(libs.plugins.analyze)
}

dependencies {
	testImplementation(libs.bundles.junit.impl)
	testRuntimeOnly(libs.junit.jupiter.engine)
	testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.withType<Test>().configureEach {
	useJUnitPlatform()
}
