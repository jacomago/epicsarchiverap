plugins {
	`java-library`
	application
	alias(libs.plugins.analyze)
}

dependencies {
	implementation(project(":shared"))
	implementation(libs.commons.cli)
	implementation(libs.commons.compress)
}

application {
	mainClass.set("org.epics.archiverappliance.pbutils.PBUtils")
}
