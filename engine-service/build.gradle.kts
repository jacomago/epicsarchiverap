plugins {
	`java-library`
	war
	alias(libs.plugins.analyze)
}

dependencies {
	implementation(project(":shared"))
	implementation(libs.tomcat.servlet.api)
}

tasks.register<War>("engineWar") {
	group = "Wars"
	description = "Builds engine.war."
	listOf("linux-x86" to "linux-x86", "linux-x86_64" to "linux-x86_64").forEach { (src, dest) ->
		from(rootProject.file("appliance/lib/native/$src")) {
			include("caRepeater")
			filePermissions {
				user { read = true; write = true; execute = true }
				group { read = true; execute = true }
				other { read = true; execute = true }
			}
			into("WEB-INF/lib/native/$dest")
		}
	}
	from(rootProject.file("appliance/lib/native")) {
		include("**/*.so")
		filePermissions {
			user { read = true; write = true; execute = true }
			group { read = true; execute = true }
			other { read = true; execute = true }
		}
		into("WEB-INF/lib/native/linux-x86")
	}
	archiveFileName.set("engine.war")
}
