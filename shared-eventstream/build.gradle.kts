plugins {
    `java-library`
    alias(libs.plugins.analyze)
}

dependencies {
    api(project(":shared"))
}
