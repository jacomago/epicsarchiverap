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
}

java {
	toolchain {
		languageVersion.set(JavaLanguageVersion.of(21))
	}
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

}

// =================================================================
// Protobuf
// =================================================================

protobuf {
	protoc {
		artifact = "com.google.protobuf:protoc:${libs.versions.protobufJava.get()}"
	}
}

