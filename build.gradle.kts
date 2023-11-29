import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent

plugins {
    kotlin("jvm") version "1.7.22"
    id("java-library")
    id("maven-publish")
    id("signing")
    id("jacoco-report-aggregation")
    jacoco
}

group = "org.vechain"

val projectVersion = System.getenv("PROJECT_VERSION") ?: "2.0.0"
version = projectVersion

repositories {
    mavenCentral()
    // local .m2 repo
    mavenLocal()
}

java {
    withJavadocJar()
    withSourcesJar()
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            artifactId = "indexer-core"
            from(components["java"])

            versionMapping {
                usage("java-api") {
                    fromResolutionOf("runtimeClasspath")
                }
                usage("java-runtime") {
                    fromResolutionResult()
                }
            }

            pom {
                name.set("indexer-core")
                description.set("This package contains an abstract VeChainThor indexer class. This class can be extended to create a custom indexer.")
                url.set("https://github.com/vechainfoundation/indexer-core")
                licenses {
                    license {
                        name.set("GNU Lesser General Public License v3.0")
                        url.set("https://github.com/vechainfoundation/indexer-core/blob/main/LICENSE")
                    }
                }
                developers {
                    developer {
                        id.set("VeWorldDeveloper")
                        name.set("VeWorld Developer")
                        email.set("veworlddeveloper@gmail.com")
                    }
                }
                scm {
                    connection.set("scm:git:git://github.com/vechainfoundation/indexer-core.git")
                    developerConnection.set("scm:git:ssh://github.com/vechainfoundation/indexer-core.git")
                    url.set("https://github.com/vechainfoundation/indexer-core")
                }
            }
        }
    }

    repositories {
        maven {
            credentials {
                username = project.properties["ossrhUsername"].toString()
                password = project.properties["ossrhPassword"].toString()
            }
            val releasesRepoUrl = uri("https://s01.oss.sonatype.org/service/local/staging/deploy/maven2/")
            val snapshotsRepoUrl = uri("https://s01.oss.sonatype.org/content/repositories/snapshots/")
            url = if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl
        }
    }
}

signing {
    // Check if the current task is not 'publishToMavenLocal'
    if (!gradle.startParameter.taskNames.contains("publishToMavenLocal")) {
        sign(publishing.publications.getByName("mavenJava"))
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
    testLogging {
        events(
            TestLogEvent.FAILED,
            TestLogEvent.PASSED,
            TestLogEvent.SKIPPED,
            TestLogEvent.STANDARD_ERROR,
            TestLogEvent.STANDARD_OUT
        )
        exceptionFormat = TestExceptionFormat.FULL
        showCauses = true
        showExceptions = true
        showStackTraces = true
    }
    finalizedBy(tasks.jacocoTestReport)
}

tasks.register<JacocoReport>("codeCoverageReport") {
    // If a subproject applies the 'jacoco' plugin, add the result it to the report
    subprojects {
        val subproject = this
        subproject.plugins.withType<JacocoPlugin>().configureEach {
            subproject.tasks
                .matching { it.extensions.findByType<JacocoTaskExtension>() != null }
                .configureEach {
                    val testTask = this
                    sourceSets(subproject.sourceSets.main.get())
                    executionData(testTask)
                }
        }
    }

    reports {
        xml.required.set(true)
        html.required.set(true)
        csv.required.set(false)
    }
}

tasks.jacocoTestReport { dependsOn(tasks.test) }

dependencies {
    implementation("com.github.kittinunf.fuel:fuel:2.3.1")
    implementation("com.github.kittinunf.fuel:fuel-coroutines:2.3.1")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.14.2")
    implementation("org.slf4j:slf4j-api:1.7.32")

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.9.3")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.7.1")
    testImplementation("io.mockk:mockk:1.13.5")
    testImplementation("io.strikt:strikt-core:0.34.1")

    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.9.3")
}