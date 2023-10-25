import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    java
    `java-library`
    application
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    api("org.apache.calcite:calcite-core:1.35.0")
    api("org.json:json:20231013")
    api("org.apache.httpcomponents.client5:httpclient5:5.2.1")

    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.apache.calcite:calcite-core:1.35.0")
}

tasks.test {
    useJUnitPlatform()
}


tasks.withType<ShadowJar> {
    archiveFileName.set("app.jar")
    manifest {
        attributes["mainClassName"] = "org.example.Main"
    }
}

application {
    mainClass = "org.example.Main"
}