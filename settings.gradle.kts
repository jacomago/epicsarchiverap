rootProject.name = "archiver-appliance"

pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

// Core shared library (Strategy A: holds all interconnected source)
include("shared")

// Shared event-stream layer: retrieval/client SPI + PBOverHTTP storage plugin
include("shared-eventstream")

// WAR services — thin BPL layer only during Phase 2
include("engine-service")
include("retrieval-service")
include("etl-service")
include("mgmt-service")

// Tooling & tests
include("pbutils")
include("integration-tests")

// Legacy monolith — retained during migration, removed in Phase 5
include("epicsarchiverap")
project(":epicsarchiverap").projectDir = file("appliance")

include("taglets")