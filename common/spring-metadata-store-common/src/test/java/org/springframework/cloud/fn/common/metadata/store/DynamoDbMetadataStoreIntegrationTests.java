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

package org.springframework.cloud.fn.common.metadata.store;

import java.net.URI;
import java.util.UUID;

import io.awspring.cloud.dynamodb.DynamoDbMetadataStore;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.fn.test.support.aws.MotoTestContainerSupport;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verify DynamoDB metadata operations with the configured SDK client against Moto.
 *
 * @author Max Brauer
 */
@Tag("integration")
public class DynamoDbMetadataStoreIntegrationTests implements MotoTestContainerSupport {

	@Test
	@Timeout(60)
	void metadataOperations() {
		String table = "sfc-metadata-" + UUID.randomUUID();
		new ApplicationContextRunner().withUserConfiguration(TestApplication.class)
			.withClassLoader(new FilteredClassLoader("com.hazelcast", "org.apache.curator",
					"org.springframework.data.mongodb", "org.springframework.data.redis", "org.springframework.jdbc"))
			.withPropertyValues(MotoTestContainerSupport.awsProperties())
			.withPropertyValues("metadata.store.type=dynamodb", "metadata.store.dynamo-db.table=" + table)
			.run((context) -> {
				assertThat(context).hasNotFailed().hasSingleBean(DynamoDbMetadataStore.class);
				DynamoDbAsyncClient client = context.getBean(DynamoDbAsyncClient.class);
				assertThat(client.serviceClientConfiguration().endpointOverride())
					.contains(URI.create(MotoTestContainerSupport.endpoint()));
				DynamoDbMetadataStore store = context.getBean(DynamoDbMetadataStore.class);

				assertThat(store.putIfAbsent("key", "one")).isNull();
				assertThat(client.listTables().join().tableNames()).contains(table);
				assertThat(store.putIfAbsent("key", "wrong")).isEqualTo("one");
				assertThat(store.get("key")).isEqualTo("one");
				assertThat(store.replace("key", "wrong", "two")).isFalse();
				assertThat(store.replace("key", "one", "two")).isTrue();
				assertThat(store.get("key")).isEqualTo("two");
				assertThat(store.remove("key")).isEqualTo("two");
				assertThat(store.get("key")).isNull();
				store.put("key", "three");
				assertThat(store.get("key")).isEqualTo("three");
				store.remove("key");
			});
	}

	@Configuration(proxyBeanMethods = false)
	@EnableAutoConfiguration
	static class TestApplication {

	}

}
