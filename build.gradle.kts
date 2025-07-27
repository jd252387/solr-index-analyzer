plugins {
    id("java")
    id("com.diffplug.spotless") version "6.25.0"
}

group = "org.example"
version = "1.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.solr:solr-core:9.9.0")
    implementation("org.apache.lucene:lucene-core:9.12.2")
    compileOnly("org.projectlombok:lombok:1.18.38")
    implementation("org.apache.lucene:lucene-backward-codecs:9.12.2")
    annotationProcessor("org.projectlombok:lombok:1.18.38")

    testCompileOnly("org.projectlombok:lombok:1.18.38")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.38")
}

tasks.test {
    useJUnitPlatform()
}

spotless {
    java {
        palantirJavaFormat()
        removeUnusedImports()
    }

    format("misc") {
        target("**/*.gradle.kts", "**/*.md", "**/.gitignore")
        trimTrailingWhitespace()
        endWithNewline()
    }
}
