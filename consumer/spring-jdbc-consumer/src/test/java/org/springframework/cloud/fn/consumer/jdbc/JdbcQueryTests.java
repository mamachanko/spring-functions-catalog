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
		this.contextRunner
			.withPropertyValues("jdbc.consumer.columns=key:headers['personId'],personName:payload",
					"jdbc.consumer.table-name=unused",
					"jdbc.consumer.query=UPDATE people SET name=:personName WHERE id=:key")
			.run((context) -> {
				JdbcTemplate jdbc = context.getBean(JdbcTemplate.class);
				jdbc.execute("CREATE TABLE people (id INTEGER PRIMARY KEY, name VARCHAR(40))");
				jdbc.update("INSERT INTO people VALUES (?, ?)", 1, "Bob");
				Consumer<Message<?>> consumer = context.getBean("jdbcConsumer", Consumer.class);
				consumer.accept(MessageBuilder.withPayload("Robert").setHeader("personId", 1).build());
				assertThat(jdbc.queryForObject("SELECT name FROM people", String.class)).isEqualTo("Robert");
				consumer.accept(MessageBuilder.withPayload("missing").setHeader("personId", 2).build());
				assertThat(jdbc.queryForObject("SELECT count(*) FROM people", Integer.class)).isEqualTo(1);
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
