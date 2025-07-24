plugins {
    kotlin("jvm") version "2.1.21"
}

group = "github.io"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.2")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-debug:1.10.2")
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(17)
}