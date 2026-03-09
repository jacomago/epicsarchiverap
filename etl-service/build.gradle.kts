plugins {
	`java-library`
	war
	alias(libs.plugins.analyze)
}

dependencies {
	implementation(project(":shared"))
	implementation(libs.tomcat.servlet.api)
}

tasks.register<War>("etlWar") {
	group = "Wars"
	description = "Builds etl.war."
	archiveFileName.set("etl.war")
}
