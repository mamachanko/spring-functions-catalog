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

package org.springframework.cloud.fn.consumer.s3;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.function.Consumer;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.fn.test.support.aws.MotoTestContainerSupport;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Verify S3 consumer auto-configuration and uploads with real clients against Moto.
 *
 * @author Max Brauer
 */
@Tag("integration")
public class AwsS3ConsumerIntegrationTests implements MotoTestContainerSupport {

	@Test
	@Timeout(60)
	@SuppressWarnings("unchecked")
	void uploadInputStream() {
		String bucket = "sfc-consumer-" + UUID.randomUUID();
		new ApplicationContextRunner().withUserConfiguration(TestApplication.class)
			.withPropertyValues(MotoTestContainerSupport.awsProperties())
			.withPropertyValues("s3.consumer.bucket=" + bucket, "s3.consumer.key-expression=headers.key")
			.run((context) -> {
				assertThat(context).hasNotFailed().hasSingleBean(S3Client.class).hasSingleBean(S3TransferManager.class);
				S3Client client = context.getBean(S3Client.class);
				assertThat(client.serviceClientConfiguration().endpointOverride())
					.contains(java.net.URI.create(MotoTestContainerSupport.endpoint()));
				client.createBucket((builder) -> builder.bucket(bucket));
				byte[] expected = "SFC S3 consumer round trip".getBytes(StandardCharsets.UTF_8);
				Consumer<Message<?>> consumer = (Consumer<Message<?>>) context.getBean("s3Consumer");

				consumer.accept(MessageBuilder.withPayload(new ByteArrayInputStream(expected))
					.setHeader("key", "probe.txt")
					.build());

				await().atMost(Duration.ofSeconds(20))
					.untilAsserted(() -> assertThat(
							client.getObjectAsBytes((builder) -> builder.bucket(bucket).key("probe.txt")).asByteArray())
						.isEqualTo(expected));
			});
	}

	@Configuration(proxyBeanMethods = false)
	@EnableAutoConfiguration
	static class TestApplication {

	}

}
