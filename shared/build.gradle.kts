plugins {
	`java-library`
	alias(libs.plugins.protobuf)
	alias(libs.plugins.analyze)
}

sourceSets {
	main {
		java { srcDir("src/main/java") }
		proto { srcDir("src/proto") }
	}
}

protobuf {
	protoc { artifact = "com.google.protobuf:protoc:${libs.versions.protobufJava.get()}" }
}

dependencies {
	// Clustering
	implementation(libs.hazelcast)
	implementation(libs.jedis)

	// Serialisation / protocol
	api(libs.protobuf.java)

	// Scripting (policy engine)
	implementation(libs.jython)

	// Validation
	implementation(libs.jakarta.validation)

	// JSON
	implementation(libs.json.simple)
	implementation(libs.json)

	// Storage
	implementation(libs.jdbm)
	runtimeOnly(libs.mariadb)
	implementation(libs.bundles.parquet)
	implementation(libs.hadoop.common)
	runtimeOnly(libs.hadoop.client.api)
	runtimeOnly(libs.hadoop.client.runtime)

	// Utilities
	implementation(libs.guava)
	implementation(libs.bundles.commons.shared)
	implementation(libs.commons.fileupload2.jakarta)
	implementation(libs.commons.fileupload2.core)
	implementation(libs.commons.compress)
	implementation(libs.commons.cli)
	implementation(libs.httpclient)
	implementation(libs.httpcore)
	implementation(libs.opencsv)

	// Logging
	implementation(libs.log4j.api)
	implementation(libs.bundles.log4j.impl)
	"permitUnusedDeclared"(libs.log4j.slf4j2)
	"permitUnusedDeclared"(libs.log4j.jul)
	runtimeOnly(libs.bundles.log4j.runtime)

	// EPICS protocol libraries
	implementation(libs.jca)
	implementation(libs.core.pva)
	implementation(files(rootProject.file("appliance/lib/jamtio_071005.jar")))
	implementation(files(rootProject.file("appliance/lib/redisnio_0.0.1.jar")))

	// Servlet API (shared contains servlet-adjacent code e.g. BasicDispatcher)
	implementation(libs.tomcat.servlet.api)

	// Policy
	implementation(libs.jinjava)
}
