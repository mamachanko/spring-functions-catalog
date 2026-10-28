/*
 * Copyright 2026-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.cloud.fn.test.support.aws;

import java.time.Duration;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Testcontainers support for local AWS service tests with Moto and dummy credentials.
 *
 * @author Max Brauer
 */
@Testcontainers(disabledWithoutDocker = true)
public interface MotoTestContainerSupport {

	/**
	 * The Moto container for local AWS API requests.
	 */
	GenericContainer<?> MOTO_CONTAINER = new GenericContainer<>("motoserver/moto:5.1.16").withExposedPorts(5000)
		.waitingFor(Wait.forHttp("/moto-api/").forStatusCode(200))
		.withStartupTimeout(Duration.ofSeconds(60));

	@BeforeAll
	static void startContainer() {
		MOTO_CONTAINER.start();
	}

	@AfterAll
	static void stopContainer() {
		MOTO_CONTAINER.stop();
	}

	static String endpoint() {
		return "http://" + MOTO_CONTAINER.getHost() + ":" + MOTO_CONTAINER.getMappedPort(5000);
	}

	static String[] awsProperties() {
		return new String[] { "spring.cloud.aws.credentials.access-key=testing",
				"spring.cloud.aws.credentials.secret-key=testing", "spring.cloud.aws.region.static=us-east-1",
				"spring.cloud.aws.endpoint=" + endpoint(), "spring.cloud.aws.s3.endpoint=" + endpoint(),
				"spring.cloud.aws.s3.path-style-access-enabled=true" };
	}

}
