/*
 * Copyright 2011-present the original author or authors.
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

package org.springframework.cloud.fn.splitter;

import java.time.Duration;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.annotation.DirtiesContext;

/**
 * Tests that the splitter function correctly handles multiple messages pushed through a
 * shared Flux, simulating how a real binder (e.g., RabbitMQ) delivers messages
 * continuously through a single reactive pipeline.
 */
@SpringBootTest(properties = "splitter.expression=payload.split(' ')")
@DirtiesContext
public class SplitterFunctionMultiMessageTests {

	@Autowired
	Function<Flux<Message<?>>, Flux<Message<?>>> splitter;

	@Test
	public void splitsSecondMessageAfterFirst() {
		Sinks.Many<Message<?>> sink = Sinks.many().unicast().onBackpressureBuffer();

		Flux<Message<?>> output = this.splitter.apply(sink.asFlux());

		Flux<String> payloads = output.map(Message::getPayload).map(Object::toString);

		StepVerifier.create(payloads)
			.then(() -> sink.tryEmitNext(new GenericMessage<>("hello world")))
			.expectNext("hello", "world")
			.then(() -> sink.tryEmitNext(new GenericMessage<>("foo bar baz")))
			.expectNext("foo", "bar", "baz")
			.then(sink::tryEmitComplete)
			.expectComplete()
			.verify(Duration.ofSeconds(10));
	}

	@Test
	public void splitsWoodchuckSentence() {
		Flux<Message<?>> output = this.splitter
			.apply(Flux.just(new GenericMessage<>("How much wood would a woodchuck chuck")));
		Flux<String> payloads = output.map(Message::getPayload).map(Object::toString);
		StepVerifier.create(payloads)
			.expectNext("How", "much", "wood", "would", "a", "woodchuck", "chuck")
			.thenCancel()
			.verify(Duration.ofSeconds(10));
	}

	@SpringBootApplication
	static class SplitterFunctionTestApplication {

	}

}
