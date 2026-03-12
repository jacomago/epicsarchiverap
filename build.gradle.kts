import com.diffplug.gradle.spotless.SpotlessExtension
import com.palantir.gradle.gitversion.VersionDetails
import org.apache.tools.ant.taskdefs.condition.Os
import java.io.FileOutputStream

plugins {
	alias(libs.plugins.git.version)
	alias(libs.plugins.spotless)
}

// =================================================================
// Project Versioning
// =================================================================

val gitVersion: groovy.lang.Closure<String> by extra
val versionDetails: groovy.lang.Closure<VersionDetails> by extra
var gitWorks = true

try {
	version = gitVersion()
} catch (e: Throwable) {
	gitWorks = false
	if (project.hasProperty("projVersion")) {
		version = project.property("projVersion") as String
	} else {
		version = "unknown"
		logger.error("Failed to get git version: $e")
	}
}

val stageDir by extra(layout.buildDirectory.dir("stage").get())
val archapplsite: String by extra(System.getenv("ARCHAPPL_SITEID") ?: "tests")
val defaultsitespecificpath = "appliance/src/sitespecific/$archapplsite"
val sitespecificpath: String by extra(
	if (file(defaultsitespecificpath).exists()) defaultsitespecificpath else archapplsite
)

// =================================================================
// Repositories (root project needs ivy for svg_viewer resolution)
// =================================================================

repositories {
	ivy {
		url = uri("https://github.com/")
		patternLayout { artifact("/[organisation]/[module]/archive/[revision].[ext]") }
		metadataSources { artifact() }
	}
}

subprojects {
	repositories {
		mavenCentral()
		flatDir {
			name = "libs dir"
			dir(rootProject.file("appliance/lib"))
		}
		flatDir {
			name = "test libs dir"
			dir(rootProject.file("appliance/lib/test"))
		}
		maven { url = uri("https://clojars.org/repo") }
		ivy {
			url = uri("https://github.com/")
			patternLayout { artifact("/[organisation]/[module]/archive/[revision].[ext]") }
			metadataSources { artifact() }
		}
	}
}

// =================================================================
// SVG Viewer dependency (used by stageSvgViewer)
// =================================================================

val viewer: Configuration by configurations.creating

dependencies {
	viewer("archiver-appliance:svg_viewer:${libs.versions.svg.viewer.get()}@zip")
}

// =================================================================
// Staging Tasks
// =================================================================

val srcDir = rootProject.file("shared/src/main/java")
val apiDocsDir = rootProject.file("docs/api")

tasks.register<Delete>("cleanApiDocs") {
	group = "Clean"
	description = "Remove the generated java api docs."
	delete(apiDocsDir)
}

tasks.named("clean") { dependsOn("cleanApiDocs") }

tasks.register<Zip>("stageSvgViewer") {
	group = "Staging"
	description = "Copy svg viewer project for assembling."
	val archPath = viewer.singleFile
	from(zipTree(archPath)) {
		include("svg_viewer-*/**")
		eachFile {
			val newPath = RelativePath(true, *relativePath.segments.drop(1).toTypedArray())
			relativePath = newPath
		}
	}
	archiveFileName.set("viewer.zip")
	includeEmptyDirs = false
	destinationDirectory.set(stageDir.asFile.resolve("org/epics/archiverappliance/retrieval/staticcontent"))
}

tasks.register<JavaExec>("syncStaticContentHeaderFooters") {
	group = "Staging"
	description = "Sync the headers and footers."
	dependsOn(":epicsarchiverap:compileJava")
	mainClass.set("org.epics.archiverappliance.mgmt.bpl.SyncStaticContentHeadersFooters")
	args(
		"$srcDir/org/epics/archiverappliance/mgmt/staticcontent/index.html",
		"$srcDir/org/epics/archiverappliance/mgmt/staticcontent/"
	)
	classpath = project(":epicsarchiverap").the<SourceSetContainer>()["main"].runtimeClasspath
}

tasks.register("sitespecificantscript") {
	group = "Staging"
	description = "Do the site specific changes from the ant script."
	dependsOn("stage")
	val serviceProjects = listOf(":engine-service", ":retrieval-service", ":etl-service", ":mgmt-service")
	ant.properties["classes"] = serviceProjects
		.map { project(it).the<SourceSetContainer>()["main"].runtimeClasspath.asPath }
		.joinToString(File.pathSeparator)
	finalizedBy(":epicsarchiverap:sitespecificbuild")
}

tasks.register("stage") {
	group = "Staging"
	description = "Copy static content from each of the projects into the staging directory."
	dependsOn(":mgmt-service:javadoc")
	finalizedBy("stageSvgViewer")
	doLast {
		stageDir.asFile.mkdirs()
		project.copy {
			from(rootProject.file("docs")) { include("*.*") }
			into(apiDocsDir)
		}
		project.copy {
			from(srcDir.resolve("org/epics/archiverappliance/staticcontent"))
			into(stageDir.asFile.resolve("org/epics/archiverappliance/staticcontent"))
		}
		project.copy {
			from(srcDir.resolve("org/epics/archiverappliance/retrieval/staticcontent"))
			into(stageDir.asFile.resolve("org/epics/archiverappliance/retrieval/staticcontent"))
		}
		project.copy {
			from(srcDir.resolve("org/epics/archiverappliance/mgmt/staticcontent"))
			into(stageDir.asFile.resolve("org/epics/archiverappliance/mgmt/staticcontent"))
		}
		stageDir.asFile
			.resolve("org/epics/archiverappliance/staticcontent/version.txt")
			.writeText("Archiver Appliance Version $version")
	}
}

tasks.register<Exec>("generateReleaseNotes") {
	group = "Staging"
	description = "Generate the Release Notes."
	outputs.file("${stageDir}/RELEASE_NOTES")
	doFirst { standardOutput = FileOutputStream("${stageDir}/RELEASE_NOTES") }
	commandLine("git", "log", "--oneline", "HEAD")
	isIgnoreExitValue = true
}

// =================================================================
// WAR Conventions (applied to all sub-projects that use the war plugin)
// =================================================================

subprojects {
	plugins.withType<WarPlugin> {
		val stageDir: Directory by rootProject.extra
		val archapplsite: String by rootProject.extra
		val sitespecificpath: String by rootProject.extra
		tasks.withType<War>().configureEach {
			dependsOn(":stage", ":sitespecificantscript")
			from(stageDir.asFile.resolve("org/epics/archiverappliance/staticcontent")) { into("ui/comm") }
			if (archapplsite == "tests") {
				from(rootProject.file("appliance/src/resources/test/log4j2.xml")) { into("WEB-INF/classes") }
			}
			from(rootProject.file(sitespecificpath).resolve("classpathfiles")) { into("WEB-INF/classes") }
			rootSpec.exclude("**/tomcat-servlet-api*.jar")
		}
	}
}

// =================================================================
// Release
// =================================================================

tasks.register<Tar>("buildRelease") {
	group = "Wars"
	description = "Builds a full release and zips up in a tar file."
	dependsOn(
		":engine-service:engineWar", ":retrieval-service:retrievalWar",
		":etl-service:etlWar", ":mgmt-service:mgmtWar", "generateReleaseNotes"
	)
	archiveFileName.set("archappl_v${version}.tar.gz")
	compression = Compression.GZIP
	from(project(":mgmt-service").layout.buildDirectory.file("libs/mgmt.war"))
	from(project(":engine-service").layout.buildDirectory.file("libs/engine.war"))
	from(project(":etl-service").layout.buildDirectory.file("libs/etl.war"))
	from(project(":retrieval-service").layout.buildDirectory.file("libs/retrieval.war"))
	from(projectDir) {
		include("LICENSE", "NOTICE", "*License.txt", "RELEASE_NOTES")
	}
	val samplesFolder = "docs/docs/source/samples"
	from(samplesFolder) {
		filePermissions {
			user { read = true; write = true; execute = true }
			group { read = true; execute = true }
			other { read = true; execute = true }
		}
		include("quickstart.sh")
	}
	from(samplesFolder) {
		include(
			"sampleStartup.sh",
			"deployMultipleTomcats.py",
			"addMysqlConnPool.py",
			"single_machine_install.sh"
		)
		into("install_scripts")
	}
	from("$samplesFolder/site_specific_content") {
		into("sample_site_specific_content")
	}
}

// =================================================================
// Code Quality and Formatting
// =================================================================

if (gitWorks) {
	configure<SpotlessExtension> {
		ratchetFrom("origin/master")
		format("misc") {
			target("*.gradle.kts", "*.md", ".gitignore")
			trimTrailingWhitespace()
			indentWithTabs()
			endWithNewline()
		}
		format("styling") {
			target(
				"docs/docs/source/**/*.html",
				"docs/docs/source/**/*.css",
				"docs/docs/source/**/*.md",
				"docs/**/docs.js",
				"appliance/src/main/**/*.html",
				"appliance/src/main/**/*.js",
				"appliance/src/main/**/*.css"
			)
			prettier()
		}
		java {
			targetExclude(fileTree(rootProject.file("appliance/src/main")) { include("**/EPICSEvent.java") })
			removeUnusedImports()
			palantirJavaFormat()
			importOrder("", "java|javax|jakarta", "#")
			formatAnnotations()
		}
	}
}

defaultTasks("buildRelease")
