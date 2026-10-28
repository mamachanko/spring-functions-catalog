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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.awaitility.Awaitility.await;

/**
 * @author Max Brauer
 */
@Testcontainers
@SuppressWarnings("unchecked")
class JdbcUpsertTests {

	private static final String POSTGRES_UPSERT = "INSERT INTO people (id,name) VALUES (:id,:name) "
			+ "ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name";

	@Container
	private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17-alpine");

	@Container
	private static final MySQLContainer MYSQL = new MySqlFixture();

	private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
		.withUserConfiguration(JdbcConsumerApplicationTests.JdbcConsumerTestApplication.class)
		.withPropertyValues("spring.sql.init.mode=never", "jdbc.consumer.columns=id,name");

	@Test
	void postgresUpsertInsertsUpdatesAndToleratesAssignmentReplay() {
		postgresRunner().withPropertyValues("jdbc.consumer.query=" + POSTGRES_UPSERT).run((context) -> {
			JdbcTemplate jdbc = context.getBean(JdbcTemplate.class);
			createPeople(jdbc);
			Consumer<Message<?>> consumer = context.getBean("jdbcConsumer", Consumer.class);
			consumer.accept(person("Bob"));
			assertThat(jdbc.queryForObject("SELECT name FROM people", String.class)).isEqualTo("Bob");
			consumer.accept(person("Robert"));
			consumer.accept(person("Robert"));
			assertThat(jdbc.queryForObject("SELECT name FROM people", String.class)).isEqualTo("Robert");
			assertThat(jdbc.queryForObject("SELECT count(*) FROM people", Integer.class)).isEqualTo(1);
			// No event version check is added: a late event overwrites the current value.
			consumer.accept(person("Bob"));
			assertThat(jdbc.queryForObject("SELECT name FROM people", String.class)).isEqualTo("Bob");
		});
	}

	@Test
	void postgresUpsertUsesJsonBytePayloadAndCountBatching() {
		postgresRunner().withPropertyValues("jdbc.consumer.batch-size=2", "jdbc.consumer.query=" + POSTGRES_UPSERT)
			.run((context) -> {
				JdbcTemplate jdbc = context.getBean(JdbcTemplate.class);
				createPeople(jdbc);
				Consumer<Message<?>> consumer = context.getBean("jdbcConsumer", Consumer.class);
				consumer
					.accept(MessageBuilder.withPayload("{\"id\":1,\"name\":\"Bob\"}".getBytes(StandardCharsets.UTF_8))
						.build());
				assertThat(jdbc.queryForObject("SELECT count(*) FROM people", Integer.class)).isZero();
				consumer.accept(
						MessageBuilder.withPayload("{\"id\":1,\"name\":\"Robert\"}".getBytes(StandardCharsets.UTF_8))
							.build());
				assertThat(jdbc.queryForObject("SELECT name FROM people", String.class)).isEqualTo("Robert");
				assertThat(jdbc.queryForObject("SELECT count(*) FROM people", Integer.class)).isEqualTo(1);
			});
	}

	@Test
	void postgresUpsertFlushesPartialBatchOnIdleTimeout() {
		postgresRunner()
			.withPropertyValues("jdbc.consumer.batch-size=10", "jdbc.consumer.idle-timeout=100",
					"jdbc.consumer.query=" + POSTGRES_UPSERT)
			.run((context) -> {
				JdbcTemplate jdbc = context.getBean(JdbcTemplate.class);
				createPeople(jdbc);
				Consumer<Message<?>> consumer = context.getBean("jdbcConsumer", Consumer.class);
				consumer.accept(person("Bob"));
				await().atMost(Duration.ofSeconds(10))
					.untilAsserted(() -> assertThat(jdbc.queryForObject("SELECT count(*) FROM people", Integer.class))
						.isEqualTo(1));
				assertThat(jdbc.queryForObject("SELECT name FROM people", String.class)).isEqualTo("Bob");
			});
	}

	@Test
	void postgresRequiresConflictConstraint() {
		postgresRunner().withPropertyValues("jdbc.consumer.query=" + POSTGRES_UPSERT).run((context) -> {
			JdbcTemplate jdbc = context.getBean(JdbcTemplate.class);
			jdbc.execute("DROP TABLE IF EXISTS people");
			jdbc.execute("CREATE TABLE people (id INTEGER, name VARCHAR(40))");
			Consumer<Message<?>> consumer = context.getBean("jdbcConsumer", Consumer.class);
			assertThatExceptionOfType(BadSqlGrammarException.class).isThrownBy(() -> consumer.accept(person("Bob")))
				.withStackTraceContaining("no unique or exclusion constraint");
			assertThat(jdbc.queryForObject("SELECT count(*) FROM people", Integer.class)).isZero();
		});
	}

	@Test
	void postgresDoesNotDeduplicateNonIdempotentUpdates() {
		postgresRunner()
			.withPropertyValues("jdbc.consumer.columns=id,increment:payload.increment",
					"jdbc.consumer.query=INSERT INTO counters (id,total) VALUES (:id,:increment) "
							+ "ON CONFLICT (id) DO UPDATE SET total=counters.total+EXCLUDED.total")
			.run((context) -> {
				JdbcTemplate jdbc = context.getBean(JdbcTemplate.class);
				jdbc.execute("CREATE TABLE counters (id INTEGER PRIMARY KEY, total INTEGER)");
				Consumer<Message<?>> consumer = context.getBean("jdbcConsumer", Consumer.class);
				Message<?> message = MessageBuilder.withPayload(Map.of("id", 1, "increment", 1)).build();
				consumer.accept(message);
				consumer.accept(message);
				assertThat(jdbc.queryForObject("SELECT total FROM counters", Integer.class)).isEqualTo(2);
			});
	}

	@Test
	void postgresQueryCanStoreJsonAndRejectOldVersions() {
		postgresRunner().withPropertyValues("jdbc.consumer.columns=id,revision,body:payload",
				"jdbc.consumer.query=INSERT INTO documents (id,revision,body) VALUES (:id,:revision,CAST(:body AS jsonb)) "
						+ "ON CONFLICT (id) DO UPDATE SET revision=EXCLUDED.revision,body=EXCLUDED.body "
						+ "WHERE EXCLUDED.revision>documents.revision")
			.run((context) -> {
				JdbcTemplate jdbc = context.getBean(JdbcTemplate.class);
				jdbc.execute("CREATE TABLE documents (id INTEGER PRIMARY KEY, revision INTEGER, body JSONB)");
				Consumer<Message<?>> consumer = context.getBean("jdbcConsumer", Consumer.class);
				String current = "{\"id\":1,\"revision\":2,\"name\":\"Robert\"}";
				consumer.accept(MessageBuilder.withPayload("{\"id\":1,\"revision\":1,\"name\":\"Bob\"}").build());
				consumer.accept(MessageBuilder.withPayload(current).build());
				consumer.accept(MessageBuilder.withPayload(current).build());
				consumer.accept(MessageBuilder.withPayload("{\"id\":1,\"revision\":1,\"name\":\"Bob\"}").build());
				assertThat(jdbc.queryForObject("SELECT body->>'name' FROM documents", String.class))
					.isEqualTo("Robert");
				assertThat(jdbc.queryForObject("SELECT revision FROM documents", Integer.class)).isEqualTo(2);
				assertThat(jdbc.queryForObject("SELECT count(*) FROM documents", Integer.class)).isEqualTo(1);
			});
	}

	@Test
	void postgresPropagatesConstraintFailures() {
		postgresRunner().withPropertyValues("jdbc.consumer.query=INSERT INTO people (id,name) VALUES (:id,:name)")
			.run((context) -> {
				JdbcTemplate jdbc = context.getBean(JdbcTemplate.class);
				createPeople(jdbc);
				Consumer<Message<?>> consumer = context.getBean("jdbcConsumer", Consumer.class);
				consumer.accept(person("Bob"));
				assertThatExceptionOfType(DataIntegrityViolationException.class)
					.isThrownBy(() -> consumer.accept(person("Robert")));
				assertThat(jdbc.queryForObject("SELECT name FROM people", String.class)).isEqualTo("Bob");
			});
	}

	@Test
	void mysqlUpsertInsertsUpdatesAndReplaysInABatch() {
		this.contextRunner
			.withPropertyValues("spring.datasource.url=" + MYSQL.getJdbcUrl(),
					"spring.datasource.username=" + MYSQL.getUsername(),
					"spring.datasource.password=" + MYSQL.getPassword(),
					"spring.datasource.driver-class-name=org.mariadb.jdbc.Driver", "jdbc.consumer.batch-size=2",
					"jdbc.consumer.query=INSERT INTO people (id,name) VALUES (:id,:name) "
							+ "ON DUPLICATE KEY UPDATE name=:name")
			.run((context) -> {
				JdbcTemplate jdbc = context.getBean(JdbcTemplate.class);
				createPeople(jdbc);
				Consumer<Message<?>> consumer = context.getBean("jdbcConsumer", Consumer.class);
				consumer.accept(person("Bob"));
				assertThat(jdbc.queryForObject("SELECT count(*) FROM people", Integer.class)).isZero();
				consumer.accept(person("Robert"));
				assertThat(jdbc.queryForObject("SELECT name FROM people", String.class)).isEqualTo("Robert");
				consumer.accept(person("Robert"));
				consumer.accept(person("Robert"));
				assertThat(jdbc.queryForObject("SELECT name FROM people", String.class)).isEqualTo("Robert");
				assertThat(jdbc.queryForObject("SELECT count(*) FROM people", Integer.class)).isEqualTo(1);
			});
	}

	private ApplicationContextRunner postgresRunner() {
		return this.contextRunner.withPropertyValues("spring.datasource.url=" + POSTGRES.getJdbcUrl(),
				"spring.datasource.username=" + POSTGRES.getUsername(),
				"spring.datasource.password=" + POSTGRES.getPassword());
	}

	private static void createPeople(JdbcTemplate jdbc) {
		jdbc.execute("DROP TABLE IF EXISTS people");
		jdbc.execute("CREATE TABLE people (id INTEGER PRIMARY KEY, name VARCHAR(40))");
	}

	private static Message<?> person(String name) {
		return MessageBuilder.withPayload(Map.of("id", 1, "name", name)).build();
	}

	private static final class MySqlFixture extends MySQLContainer {

		private MySqlFixture() {
			super("mysql:8.0");
			withUrlParam("permitMysqlScheme", "true");
			withUrlParam("sslMode", "trust");
		}

		@Override
		public String getDriverClassName() {
			return "org.mariadb.jdbc.Driver";
		}

	}

}
