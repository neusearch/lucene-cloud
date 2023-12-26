plugins {
    id("java")
}

group = "io.neusearch"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.lucene:lucene-core:9.8.0")
    implementation(platform("software.amazon.awssdk:bom:2.22.5"))
    implementation("software.amazon.awssdk:s3")
    implementation("software.amazon.awssdk:s3-transfer-manager")
    implementation("software.amazon.awssdk.crt:aws-crt:0.29.1")
    implementation("org.slf4j:slf4j-log4j12:2.0.7")
    implementation("commons-io:commons-io:2.13.0")
    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.mockito:mockito-core:5.8.0")
}

tasks.test {
    useJUnitPlatform()
}