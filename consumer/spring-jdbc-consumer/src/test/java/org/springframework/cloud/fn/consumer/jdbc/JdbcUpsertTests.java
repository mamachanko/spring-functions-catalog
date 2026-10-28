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

	private static final String POSTGRES_UPSERT = "INSERT INTO claims (id,status) VALUES (:id,:status) "
			+ "ON CONFLICT (id) DO UPDATE SET status=EXCLUDED.status";

	@Container
	private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17-alpine");

	@Container
	private static final MySQLContainer MYSQL = new MySqlFixture();

	private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
		.withUserConfiguration(JdbcConsumerApplicationTests.JdbcConsumerTestApplication.class)
		.withPropertyValues("spring.sql.init.mode=never", "jdbc.consumer.columns=id,status");

	@Test
	void postgresUpsertInsertsUpdatesAndToleratesAssignmentReplay() {
		postgresRunner().withPropertyValues("jdbc.consumer.query=" + POSTGRES_UPSERT).run((context) -> {
			JdbcTemplate jdbc = context.getBean(JdbcTemplate.class);
			createClaims(jdbc);
			Consumer<Message<?>> consumer = context.getBean("jdbcConsumer", Consumer.class);
			consumer.accept(claim("new"));
			assertThat(jdbc.queryForObject("SELECT status FROM claims", String.class)).isEqualTo("new");
			consumer.accept(claim("paid"));
			consumer.accept(claim("paid"));
			assertThat(jdbc.queryForObject("SELECT status FROM claims", String.class)).isEqualTo("paid");
			assertThat(jdbc.queryForObject("SELECT count(*) FROM claims", Integer.class)).isEqualTo(1);
			// No event version check is added: a late event overwrites the current value.
			consumer.accept(claim("new"));
			assertThat(jdbc.queryForObject("SELECT status FROM claims", String.class)).isEqualTo("new");
		});
	}

	@Test
	void postgresUpsertUsesJsonBytePayloadAndCountBatching() {
		postgresRunner().withPropertyValues("jdbc.consumer.batch-size=2", "jdbc.consumer.query=" + POSTGRES_UPSERT)
			.run((context) -> {
				JdbcTemplate jdbc = context.getBean(JdbcTemplate.class);
				createClaims(jdbc);
				Consumer<Message<?>> consumer = context.getBean("jdbcConsumer", Consumer.class);
				consumer.accept(MessageBuilder
					.withPayload("{\"id\":\"C-17\",\"status\":\"new\"}".getBytes(StandardCharsets.UTF_8))
					.build());
				assertThat(jdbc.queryForObject("SELECT count(*) FROM claims", Integer.class)).isZero();
				consumer.accept(MessageBuilder
					.withPayload("{\"id\":\"C-17\",\"status\":\"paid\"}".getBytes(StandardCharsets.UTF_8))
					.build());
				assertThat(jdbc.queryForObject("SELECT status FROM claims", String.class)).isEqualTo("paid");
				assertThat(jdbc.queryForObject("SELECT count(*) FROM claims", Integer.class)).isEqualTo(1);
			});
	}

	@Test
	void postgresUpsertFlushesPartialBatchOnIdleTimeout() {
		postgresRunner()
			.withPropertyValues("jdbc.consumer.batch-size=10", "jdbc.consumer.idle-timeout=100",
					"jdbc.consumer.query=" + POSTGRES_UPSERT)
			.run((context) -> {
				JdbcTemplate jdbc = context.getBean(JdbcTemplate.class);
				createClaims(jdbc);
				Consumer<Message<?>> consumer = context.getBean("jdbcConsumer", Consumer.class);
				consumer.accept(claim("new"));
				await().atMost(Duration.ofSeconds(10))
					.untilAsserted(() -> assertThat(jdbc.queryForObject("SELECT count(*) FROM claims", Integer.class))
						.isEqualTo(1));
				assertThat(jdbc.queryForObject("SELECT status FROM claims", String.class)).isEqualTo("new");
			});
	}

	@Test
	void postgresRequiresConflictConstraint() {
		postgresRunner().withPropertyValues("jdbc.consumer.query=" + POSTGRES_UPSERT).run((context) -> {
			JdbcTemplate jdbc = context.getBean(JdbcTemplate.class);
			jdbc.execute("DROP TABLE IF EXISTS claims");
			jdbc.execute("CREATE TABLE claims (id VARCHAR(40), status VARCHAR(40))");
			Consumer<Message<?>> consumer = context.getBean("jdbcConsumer", Consumer.class);
			assertThatExceptionOfType(BadSqlGrammarException.class).isThrownBy(() -> consumer.accept(claim("new")))
				.withStackTraceContaining("no unique or exclusion constraint");
			assertThat(jdbc.queryForObject("SELECT count(*) FROM claims", Integer.class)).isZero();
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
				jdbc.execute("CREATE TABLE counters (id VARCHAR(40) PRIMARY KEY, total INTEGER)");
				Consumer<Message<?>> consumer = context.getBean("jdbcConsumer", Consumer.class);
				Message<?> message = MessageBuilder.withPayload(Map.of("id", "C-17", "increment", 1)).build();
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
				jdbc.execute("CREATE TABLE documents (id VARCHAR(40) PRIMARY KEY, revision INTEGER, body JSONB)");
				Consumer<Message<?>> consumer = context.getBean("jdbcConsumer", Consumer.class);
				String current = "{\"id\":\"C-17\",\"revision\":2,\"status\":\"paid\"}";
				consumer
					.accept(MessageBuilder.withPayload("{\"id\":\"C-17\",\"revision\":1,\"status\":\"new\"}").build());
				consumer.accept(MessageBuilder.withPayload(current).build());
				consumer.accept(MessageBuilder.withPayload(current).build());
				consumer
					.accept(MessageBuilder.withPayload("{\"id\":\"C-17\",\"revision\":1,\"status\":\"new\"}").build());
				assertThat(jdbc.queryForObject("SELECT body->>'status' FROM documents", String.class))
					.isEqualTo("paid");
				assertThat(jdbc.queryForObject("SELECT revision FROM documents", Integer.class)).isEqualTo(2);
				assertThat(jdbc.queryForObject("SELECT count(*) FROM documents", Integer.class)).isEqualTo(1);
			});
	}

	@Test
	void postgresPropagatesConstraintFailures() {
		postgresRunner().withPropertyValues("jdbc.consumer.query=INSERT INTO claims (id,status) VALUES (:id,:status)")
			.run((context) -> {
				JdbcTemplate jdbc = context.getBean(JdbcTemplate.class);
				createClaims(jdbc);
				Consumer<Message<?>> consumer = context.getBean("jdbcConsumer", Consumer.class);
				consumer.accept(claim("new"));
				assertThatExceptionOfType(DataIntegrityViolationException.class)
					.isThrownBy(() -> consumer.accept(claim("paid")));
				assertThat(jdbc.queryForObject("SELECT status FROM claims", String.class)).isEqualTo("new");
			});
	}

	@Test
	void mysqlUpsertInsertsUpdatesAndReplaysInABatch() {
		this.contextRunner
			.withPropertyValues("spring.datasource.url=" + MYSQL.getJdbcUrl(),
					"spring.datasource.username=" + MYSQL.getUsername(),
					"spring.datasource.password=" + MYSQL.getPassword(),
					"spring.datasource.driver-class-name=org.mariadb.jdbc.Driver", "jdbc.consumer.batch-size=2",
					"jdbc.consumer.query=INSERT INTO claims (id,status) VALUES (:id,:status) "
							+ "ON DUPLICATE KEY UPDATE status=:status")
			.run((context) -> {
				JdbcTemplate jdbc = context.getBean(JdbcTemplate.class);
				createClaims(jdbc);
				Consumer<Message<?>> consumer = context.getBean("jdbcConsumer", Consumer.class);
				consumer.accept(claim("new"));
				assertThat(jdbc.queryForObject("SELECT count(*) FROM claims", Integer.class)).isZero();
				consumer.accept(claim("paid"));
				assertThat(jdbc.queryForObject("SELECT status FROM claims", String.class)).isEqualTo("paid");
				consumer.accept(claim("paid"));
				consumer.accept(claim("paid"));
				assertThat(jdbc.queryForObject("SELECT status FROM claims", String.class)).isEqualTo("paid");
				assertThat(jdbc.queryForObject("SELECT count(*) FROM claims", Integer.class)).isEqualTo(1);
			});
	}

	private ApplicationContextRunner postgresRunner() {
		return this.contextRunner.withPropertyValues("spring.datasource.url=" + POSTGRES.getJdbcUrl(),
				"spring.datasource.username=" + POSTGRES.getUsername(),
				"spring.datasource.password=" + POSTGRES.getPassword());
	}

	private static void createClaims(JdbcTemplate jdbc) {
		jdbc.execute("DROP TABLE IF EXISTS claims");
		jdbc.execute("CREATE TABLE claims (id VARCHAR(40) PRIMARY KEY, status VARCHAR(40))");
	}

	private static Message<?> claim(String status) {
		return MessageBuilder.withPayload(Map.of("id", "C-17", "status", status)).build();
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
