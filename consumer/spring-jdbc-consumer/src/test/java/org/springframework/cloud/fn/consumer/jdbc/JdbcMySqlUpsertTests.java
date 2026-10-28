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
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.mysql.MySQLContainer;

import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Max Brauer
 */
@Testcontainers(disabledWithoutDocker = true)
@SuppressWarnings("unchecked")
class JdbcMySqlUpsertTests {

	@Container
	private static final MySQLContainer MYSQL = new MySqlFixture();

	private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
		.withUserConfiguration(JdbcConsumerApplicationTests.JdbcConsumerTestApplication.class)
		.withPropertyValues("spring.sql.init.mode=never", "jdbc.consumer.columns=id,name");

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

	private static void createPeople(JdbcTemplate jdbc) {
		jdbc.execute("DROP TABLE IF EXISTS people");
		jdbc.execute("CREATE TABLE people (id INTEGER PRIMARY KEY, name VARCHAR(40))");
	}

	private static Message<?> person(String name) {
		return MessageBuilder.withPayload(Map.of("id", 1, "name", name)).build();
	}

	// Use the module's MariaDB driver with the MySQL JDBC URL supplied by the container.
	private static final class MySqlFixture extends MySQLContainer {

		private MySqlFixture() {
			super("mysql:8.0");
			// The MariaDB driver requires this option to accept a jdbc:mysql URL.
			withUrlParam("permitMysqlScheme", "true");
			withUrlParam("sslMode", "trust");
		}

		@Override
		public String getDriverClassName() {
			return "org.mariadb.jdbc.Driver";
		}

	}

}
