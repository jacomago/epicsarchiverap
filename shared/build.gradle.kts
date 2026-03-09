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
	api(libs.hazelcast)
	api(libs.jedis)

	// Serialisation / protocol
	api(libs.protobuf.java)

	// Scripting (policy engine)
	api(libs.jython)

	// Validation
	api(libs.jakarta.validation)

	// JSON
	api(libs.json.simple)
	api(libs.json)

	// Storage
	api(libs.jdbm)
	runtimeOnly(libs.mariadb)
	api(libs.bundles.parquet)
	api(libs.hadoop.common)
	runtimeOnly(libs.hadoop.client.api)
	runtimeOnly(libs.hadoop.client.runtime)

	// Utilities
	api(libs.guava)
	api(libs.bundles.commons.shared)
	api(libs.commons.fileupload2.jakarta)
	api(libs.commons.fileupload2.core)
	api(libs.commons.compress)
	api(libs.commons.cli)
	api(libs.httpclient)
	api(libs.httpcore)
	api(libs.opencsv)

	// Logging
	api(libs.log4j.api)
	api(libs.bundles.log4j.impl)
	"permitUnusedDeclared"(libs.log4j.slf4j2)
	"permitUnusedDeclared"(libs.log4j.jul)
	runtimeOnly(libs.bundles.log4j.runtime)

	// EPICS protocol libraries
	api(libs.jca)
	api(libs.core.pva)
	api(files(rootProject.file("appliance/lib/jamtio_071005.jar")))
	api(files(rootProject.file("appliance/lib/redisnio_0.0.1.jar")))

	// Servlet API (shared contains servlet-adjacent code e.g. BasicDispatcher)
	api(libs.tomcat.servlet.api)

	// Policy
	api(libs.jinjava)
}
