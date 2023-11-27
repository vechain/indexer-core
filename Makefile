help:
	@egrep -h '\s#@\s' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?#@ "}; {printf "\033[36m  %-30s\033[0m %s\n", $$1, $$2}'

test: #@ Run all the tests.
	./gradlew cleanTest test

# Application Build
build: #@ Build the library with Gradle.
	./gradlew build
.PHONY:build

# Publishing
publish-local: #@ Publish the library to my local maven repo.
	./gradlew publishToMavenLocal
publish: #@ Publish the library to Maven Central.
	./gradlew release