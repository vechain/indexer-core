import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent

plugins {
    kotlin("jvm") version "2.1.21"
    id("java-library")
    id("maven-publish")
    id("signing")
    id("jacoco-report-aggregation")
    id("io.github.gradle-nexus.publish-plugin") version "2.0.0"
    id("com.diffplug.spotless") version "6.25.0"
    jacoco
}

java.sourceCompatibility = JavaVersion.VERSION_21

jacoco {
    toolVersion = "0.8.11"
}

group = "org.vechain"

val projectVersion = System.getenv("PROJECT_VERSION") ?: "8.0.1"
version = projectVersion

val isSnapshot = version.toString().endsWith("SNAPSHOT")

repositories {
    mavenCentral()
    // local .m2 repo
    mavenLocal()
}

spotless {
    kotlin {
        ktfmt().googleStyle().configure {
            it.setBlockIndent(4)
            it.setContinuationIndent(4)
        }
    }
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
                description.set(
                    "This package contains an abstract VeChainThor indexer class. This class can be extended to create a custom indexer.",
                )
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
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/vechain/indexer-core")
            credentials {
                username = findProperty("gpr.user") as String? ?: System.getenv("GITHUB_ACTOR")
                password = findProperty("gpr.key") as String? ?: System.getenv("GITHUB_TOKEN")
            }
        }
        maven {
            credentials {
                username = findProperty("ossrhUsername") as String?
                password = findProperty("ossrhPassword") as String?
            }
            val releasesRepoUrl = uri("https://ossrh-staging-api.central.sonatype.com/service/local/staging/deploy/maven2/")
            val snapshotsRepoUrl = uri("https://central.sonatype.com/repository/maven-snapshots/")
            url = if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl
        }
    }
}

signing {
    isRequired = gradle.taskGraph.hasTask("publish") && !gradle.taskGraph.hasTask("publishToMavenLocal")
    val signingKey = findProperty("signingKey") as String?
    val signingPassword = findProperty("signingPassword") as String?
    useInMemoryPgpKeys(signingKey, signingPassword)

    sign(publishing.publications.getByName("mavenJava"))
}

tasks.withType<Test> {
    useJUnitPlatform()
    testLogging {
        events(
            TestLogEvent.FAILED,
            TestLogEvent.PASSED,
            TestLogEvent.SKIPPED,
            TestLogEvent.STANDARD_ERROR,
            TestLogEvent.STANDARD_OUT,
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
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:2.18.3")
    implementation("org.slf4j:slf4j-api:2.0.13")
    implementation("org.bouncycastle:bcprov-jdk15on:1.70")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.7.1")

    // Version constraints for transitive dependencies
    constraints {
        implementation("com.fasterxml.jackson.core:jackson-core:2.18.6")
    }

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.10.2")
    testImplementation("org.jetbrains.kotlin:kotlin-test:2.1.21")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.7.1")
    testImplementation("io.mockk:mockk:1.13.10")
    testImplementation("io.strikt:strikt-core:0.34.1")
    testImplementation("org.slf4j:slf4j-simple:2.0.13")

    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.10.2")
}

if (!isSnapshot) {
    nexusPublishing {
        repositories {
            sonatype {
                nexusUrl.set(uri("https://ossrh-staging-api.central.sonatype.com/service/local/"))
                username.set(findProperty("ossrhUsername") as String?)
                password.set(findProperty("ossrhPassword") as String?)
            }
        }
    }
}

dependencyLocking {
    lockAllConfigurations()
}
