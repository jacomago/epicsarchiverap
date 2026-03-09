plugins {
	`java-library`
	war
	alias(libs.plugins.analyze)
}

dependencies {
	implementation(project(":shared"))
	implementation(libs.tomcat.servlet.api)
}

tasks.register<War>("retrievalWar") {
	group = "Wars"
	description = "Builds retrieval.war."
	dependsOn(":stageSvgViewer")
	archiveFileName.set("retrieval.war")
}
