import com.palantir.gradle.gitversion.VersionDetails
import org.apache.tools.ant.taskdefs.condition.Os

plugins {
	`java-library`
	war
	eclipse
	alias(libs.plugins.analyze)
	alias(libs.plugins.protobuf)
}

// =================================================================
// Source Sets
// =================================================================

sourceSets {
	main {
		java {
			srcDir("src/main/")
		}
		proto {
			srcDir("src/proto")
		}
		resources {
			setSrcDirs(emptyList<String>())
		}
	}
	test {
		java {
			srcDir("src/test/")
		}
		resources {
			setSrcDirs(emptyList<String>())
		}
	}
}

java {
	toolchain {
		languageVersion.set(JavaLanguageVersion.of(21))
	}
}

val runTestsSequentially: Boolean by extra {
	(findProperty("ARCHAPPL_SEQUENTIAL_TESTS") as? String)?.toBoolean() ?: false
}

// =================================================================
// Ant build (provides sitespecificbuild target)
// =================================================================

val stageDir: Directory by rootProject.extra
val archapplsite: String by rootProject.extra
val sitespecificpath: String by rootProject.extra

ant.properties["stage"] = stageDir
ant.properties["archapplsite"] = archapplsite
ant.properties["sitespecificpath"] = sitespecificpath
ant.importBuild("build.xml")

// =================================================================
// Dependencies
// =================================================================

dependencies {
	// Local JARs
	implementation(files("lib/jamtio_071005.jar", "lib/redisnio_0.0.1.jar"))

	// Provided by Servlet Container (e.g., Tomcat)
	implementation(libs.tomcat.servlet.api)

	// Core Libraries
	implementation(libs.jca)
	implementation(libs.guava)
	implementation(libs.hazelcast)
	implementation(libs.jedis)
	implementation(libs.jython)
	implementation(libs.protobuf.java)
	implementation(libs.core.pva)
	implementation(libs.jakarta.validation)

	// Apache Commons
	implementation(libs.commons.codec)
	implementation(libs.commons.fileupload2.jakarta)
	implementation(libs.commons.fileupload2.core)
	implementation(libs.commons.io)
	implementation(libs.commons.lang3)
	implementation(libs.commons.math3)
	implementation(libs.commons.validator)

	// HTTP Clients
	implementation(libs.httpclient)
	implementation(libs.httpcore)

	// Data Formats & DB
	implementation(libs.jdbm)
	implementation(libs.json.simple)
	implementation(libs.json)
	implementation(libs.opencsv)
	runtimeOnly(libs.mariadb)

	// Logging
	runtimeOnly(libs.log4j.one.two.api)
	implementation(libs.log4j.api)
	implementation(libs.log4j.slf4j2)
	"permitUnusedDeclared"(libs.log4j.slf4j2)
	implementation(libs.log4j.jul)
	"permitUnusedDeclared"(libs.log4j.jul)
	"permitUsedUndeclared"(libs.stax.api)
	runtimeOnly(libs.log4j.core)
	runtimeOnly(libs.disruptor)

	// Parquet support
	implementation(libs.parquet.protobuf)
	implementation(libs.parquet.column)
	implementation(libs.parquet.common)
	implementation(libs.parquet.hadoop)
	implementation(libs.hadoop.common)
	runtimeOnly(libs.hadoop.client.api)
	runtimeOnly(libs.hadoop.client.runtime)

	// Testing
	testImplementation(libs.junit.jupiter.api)
	testImplementation(libs.junit.jupiter.params)
	testRuntimeOnly(libs.junit.jupiter.engine)
	testRuntimeOnly(libs.junit.platform.launcher)
	testImplementation(libs.awaitility)
	testImplementation(libs.commons.compress)
	testImplementation(libs.commons.cli)
	testImplementation(libs.jinjava)
	testImplementation(project(":taglets"))
	testImplementation(":pbrawclient:0.2.2")
	testImplementation(libs.tomcat.servlet.api)
	testImplementation(libs.mockito)
}

// =================================================================
// Protobuf
// =================================================================

protobuf {
	protoc {
		artifact = "com.google.protobuf:protoc:${libs.versions.protobufJava.get()}"
	}
}

// =================================================================
// Test Tasks
// =================================================================

tasks.withType<Test>().configureEach {
	testClassesDirs = sourceSets.test.get().output.classesDirs
	classpath = sourceSets.test.get().runtimeClasspath

	maxParallelForks = if (runTestsSequentially) {
		1
	} else {
		(Runtime.getRuntime().availableProcessors() / 2).coerceAtLeast(1)
	}

	doFirst {
		temporaryDir.resolve("sts").mkdirs()
		temporaryDir.resolve("mts").mkdirs()
		temporaryDir.resolve("lts").mkdirs()
		logger.lifecycle("Running tests with maxParallelForks = {}", maxParallelForks)
	}

	filter {
		includeTestsMatching("*Test")
	}

	maxHeapSize = "1G"
	jvmArgs = listOf("-Dlog4j1.compatibility=true")

	environment("ARCHAPPL_SHORT_TERM_FOLDER", temporaryDir.resolve("sts").path)
	environment("ARCHAPPL_MEDIUM_TERM_FOLDER", temporaryDir.resolve("mts").path)
	environment("ARCHAPPL_LONG_TERM_FOLDER", temporaryDir.resolve("lts").path)

	doLast {
		delete(
			temporaryDir.resolve("sts"),
			temporaryDir.resolve("mts"),
			temporaryDir.resolve("lts")
		)
	}
}

tasks.named<ProcessResources>("processTestResources") {
	from(layout.projectDirectory.file("src/sitespecific/tests/classpathfiles"))
	from(layout.projectDirectory.file("src/resources/test")) {
		include("log4j2.xml")
		include("appliances.xml.j2")
		include("log4j2.component.properties")
	}
}

tasks.register<Test>("unitTests") {
	group = "Test"
	description = "Run all unit tests."
	useJUnitPlatform {
		excludeTags("flaky", "integration", "localEpics")
	}
}

tasks.register<Test>("flakyTests") {
	group = "Test"
	description = "Run unit tests that due to timing or machine specifics, could fail."
	useJUnitPlatform {
		includeTags("flaky")
		excludeTags("slow", "integration", "localEpics")
	}
}

tasks.register<Exec>("shutdownAllTomcats") {
	group = "Test"
	description = "Task to shut down all tomcats after running integration tests."
	setIgnoreExitValue(true)
	if (!Os.isFamily(Os.FAMILY_WINDOWS)) {
		commandLine("pkill", "-9", "-f", "Deaatag=eaatesttm")
	} else {
		doFirst {
			logger.warn("pkill for tomcat shutdown is not supported on Windows. Skipping.")
		}
	}
}

tasks.register("integrationTestSetup") {
	group = "Test"
	description = "Setup for Integration Tests by backing up Tomcat's conf directory."
	dependsOn(":buildRelease")

	val tomcatHome = System.getenv("TOMCAT_HOME")
	onlyIf {
		if (tomcatHome == null) {
			logger.warn("TOMCAT_HOME environment variable is not set. Skipping integration test setup.")
			false
		} else {
			true
		}
	}

	doLast {
		val tomcatConfOriginal = file("$tomcatHome/conf_original")
		if (!tomcatConfOriginal.exists()) {
			logger.lifecycle("Backing up Tomcat configuration to $tomcatConfOriginal")
			project.copy {
				from("$tomcatHome/conf")
				into(tomcatConfOriginal)
			}
		} else {
			logger.info("Tomcat configuration backup already exists at $tomcatConfOriginal")
		}
	}
}

tasks.register<Test>("integrationTests") {
	group = "Test"
	description = "Run the integration tests, ones that require a tomcat installation."
	forkEvery = 1
	maxParallelForks = 1
	dependsOn("integrationTestSetup")
	useJUnitPlatform {
		includeTags("integration")
		excludeTags("slow", "flaky")
	}
	finalizedBy("shutdownAllTomcats")
}

tasks.register<Test>("epicsTests") {
	group = "Test"
	description = "Run the epics integration tests with parallel iocs."
	useJUnitPlatform {
		includeTags("localEpics")
		excludeTags("slow", "flaky", "integration")
	}
}

tasks.register<Test>("singleForkTests") {
	group = "Test"
	description = "Run the single fork tests."
	forkEvery = 1
	useJUnitPlatform {
		includeTags("singleFork")
		excludeTags("slow", "flaky", "integration", "localEpics")
	}
}

tasks.named<Test>("test") {
	group = "Test"
	useJUnitPlatform {
		excludeTags("integration", "localEpics", "flaky", "singleFork", "slow")
	}
}

tasks.register("allTests") {
	group = "Verification"
	description = "Run all the tests."
	dependsOn("unitTests", "singleForkTests", "integrationTests", "flakyTests", "epicsTests")
}

tasks.register<Test>("automationTests") {
	group = "Verification"
	description = "Run all the tests in an automated environment."
	forkEvery = 1
	maxParallelForks = 1
	dependsOn("integrationTestSetup")
	useJUnitPlatform {}
	finalizedBy("shutdownAllTomcats")
}

tasks.register<JavaExec>("testRun") {
	group = "Test"
	description = "Runs the application same as for integration tests."
	dependsOn("integrationTestSetup")

	environment("ARCHAPPL_SHORT_TERM_FOLDER", "${layout.buildDirectory.get()}/storage/sts")
	environment("ARCHAPPL_MEDIUM_TERM_FOLDER", "${layout.buildDirectory.get()}/storage/mts")
	environment("ARCHAPPL_LONG_TERM_FOLDER", "${layout.buildDirectory.get()}/storage/lts")

	mainClass.set("org.epics.archiverappliance.TestRun")
	classpath = sourceSets.test.get().runtimeClasspath
	args("-c2")
}
