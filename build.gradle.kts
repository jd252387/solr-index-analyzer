plugins {
    id("java")
}

group = "org.example"
version = "1.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.solr:solr-core:9.8.1")
    implementation("org.apache.lucene:lucene-core:9.11.1")
    compileOnly("org.projectlombok:lombok:1.18.38")
    implementation("org.apache.lucene:lucene-backward-codecs:9.11.1")
    annotationProcessor("org.projectlombok:lombok:1.18.38")

    testCompileOnly("org.projectlombok:lombok:1.18.38")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.38")
}

tasks.test {
    useJUnitPlatform()
}