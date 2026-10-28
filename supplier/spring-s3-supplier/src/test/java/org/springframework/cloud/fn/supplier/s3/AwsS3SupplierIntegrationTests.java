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

package org.springframework.cloud.fn.supplier.s3;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.function.Supplier;

import io.awspring.cloud.s3.integration.S3InboundFileSynchronizingMessageSource;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Flux;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.fn.test.support.aws.MotoTestContainerSupport;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verify S3 supplier auto-configuration, downloads and listings against Moto.
 *
 * @author Max Brauer
 */
@Tag("integration")
public class AwsS3SupplierIntegrationTests implements MotoTestContainerSupport {

	@Test
	@Timeout(60)
	void downloadAndFilterDuplicates(@TempDir Path directory) {
		String bucket = "sfc-supplier-" + UUID.randomUUID();
		contextRunner(bucket).withPropertyValues("s3.supplier.local-dir=" + directory).run((context) -> {
			assertThat(context).hasNotFailed();
			S3Client client = context.getBean(S3Client.class);
			client.createBucket((builder) -> builder.bucket(bucket));
			byte[] expected = "SFC S3 supplier download".getBytes(StandardCharsets.UTF_8);
			client.putObject((builder) -> builder.bucket(bucket).key("download.txt"), RequestBody.fromBytes(expected));
			S3InboundFileSynchronizingMessageSource source = context.getBean("s3MessageSource",
					S3InboundFileSynchronizingMessageSource.class);
			source.start();

			await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(source.receive()).isNotNull());

			assertThat(Files.readAllBytes(directory.resolve("download.txt"))).isEqualTo(expected);
			assertThat(source.receive()).isNull();
		});
	}

	@Test
	@Timeout(60)
	@SuppressWarnings("unchecked")
	void listObjectsAsJson() {
		String bucket = "sfc-list-" + UUID.randomUUID();
		contextRunner(bucket).withPropertyValues("s3.supplier.list-only=true").run((context) -> {
			assertThat(context).hasNotFailed();
			S3Client client = context.getBean(S3Client.class);
			client.createBucket((builder) -> builder.bucket(bucket));
			client.putObject((builder) -> builder.bucket(bucket).key("list.txt"), RequestBody.fromString("listing"));
			Supplier<Flux<Message<?>>> supplier = (Supplier<Flux<Message<?>>>) context.getBean("s3Supplier");

			Message<?> result = supplier.get().next().block(Duration.ofSeconds(15));

			assertThat(result).isNotNull();
			assertThat(result.getPayload().toString()).contains("list.txt");
		});
	}

	private ApplicationContextRunner contextRunner(String bucket) {
		return new ApplicationContextRunner().withUserConfiguration(TestApplication.class)
			.withPropertyValues(MotoTestContainerSupport.awsProperties())
			.withPropertyValues("s3.supplier.remote-dir=" + bucket);
	}

	@Configuration(proxyBeanMethods = false)
	@EnableAutoConfiguration
	static class TestApplication {

	}

}
