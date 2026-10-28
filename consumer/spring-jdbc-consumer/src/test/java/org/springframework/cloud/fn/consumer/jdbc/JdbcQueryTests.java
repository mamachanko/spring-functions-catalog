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

package org.springframework.cloud.fn.consumer.jdbc;

import java.util.Map;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/**
 * @author Max Brauer
 */
@SuppressWarnings("unchecked")
class JdbcQueryTests {

	private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
		.withUserConfiguration(JdbcConsumerApplicationTests.JdbcConsumerTestApplication.class)
		.withPropertyValues("spring.datasource.generate-unique-name=true", "spring.sql.init.mode=never");

	@ParameterizedTest
	@ValueSource(strings = { "", "   " })
	void blankQueryGeneratesInsert(String query) {
		this.contextRunner.withPropertyValues("jdbc.consumer.initialize=true")
			.withInitializer((context) -> TestPropertyValues.of(Map.of("jdbc.consumer.query", query)).applyTo(context))
			.run((context) -> {
				assertThat(context.getBean(JdbcConsumerProperties.class).getQuery()).isEqualTo(query);
				Consumer<Message<?>> consumer = context.getBean("jdbcConsumer", Consumer.class);
				consumer.accept(MessageBuilder.withPayload("hello").build());
				assertThat(context.getBean(JdbcTemplate.class)
					.queryForObject("SELECT payload FROM messages", String.class)).isEqualTo("hello");
			});
	}

	@Test
	void updateUsesNamedParametersFromHeadersAndPayloadWithoutInsertFallback() {
		this.contextRunner.withPropertyValues("jdbc.consumer.columns=key:headers['key'],state:payload",
				"jdbc.consumer.table-name=unused", "jdbc.consumer.query=UPDATE claims SET status=:state WHERE id=:key")
			.run((context) -> {
				JdbcTemplate jdbc = context.getBean(JdbcTemplate.class);
				jdbc.execute("CREATE TABLE claims (id VARCHAR(40) PRIMARY KEY, status VARCHAR(40))");
				jdbc.update("INSERT INTO claims VALUES (?, ?)", "C-17", "new");
				Consumer<Message<?>> consumer = context.getBean("jdbcConsumer", Consumer.class);
				consumer.accept(MessageBuilder.withPayload("paid").setHeader("key", "C-17").build());
				assertThat(jdbc.queryForObject("SELECT status FROM claims", String.class)).isEqualTo("paid");
				consumer.accept(MessageBuilder.withPayload("missing").setHeader("key", "C-18").build());
				assertThat(jdbc.queryForObject("SELECT count(*) FROM claims", Integer.class)).isEqualTo(1);
			});
	}

	@Test
	void unmappedParameterPropagatesFailure() {
		this.contextRunner
			.withPropertyValues("jdbc.consumer.initialize=true",
					"jdbc.consumer.query=INSERT INTO messages (payload) VALUES (:missing)")
			.run((context) -> {
				Consumer<Message<?>> consumer = context.getBean("jdbcConsumer", Consumer.class);
				assertThatExceptionOfType(InvalidDataAccessApiUsageException.class)
					.isThrownBy(() -> consumer.accept(MessageBuilder.withPayload("hello").build()))
					.withStackTraceContaining("missing");
				assertThat(context.getBean(JdbcTemplate.class)
					.queryForObject("SELECT count(*) FROM messages", Integer.class)).isZero();
			});
	}

}
