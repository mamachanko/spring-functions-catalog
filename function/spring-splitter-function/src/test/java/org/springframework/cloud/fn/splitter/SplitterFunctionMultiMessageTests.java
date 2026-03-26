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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.annotation.DirtiesContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that the splitter function correctly handles multiple messages invoked
 * sequentially, simulating how a real binder delivers messages one at a time.
 */
@SpringBootTest(properties = "splitter.expression=payload.split(' ')")
@DirtiesContext
public class SplitterFunctionMultiMessageTests {

	@Autowired
	Function<Message<?>, List<Message<?>>> splitter;

	@Test
	public void splitsMultipleMessagesSequentially() {
		List<String> allResults = new ArrayList<>();

		List<Message<?>> first = this.splitter.apply(new GenericMessage<>("hello world"));
		first.stream().map(Message::getPayload).map(Object::toString).forEach(allResults::add);

		List<Message<?>> second = this.splitter.apply(new GenericMessage<>("foo bar baz"));
		second.stream().map(Message::getPayload).map(Object::toString).forEach(allResults::add);

		assertThat(allResults).containsExactly("hello", "world", "foo", "bar", "baz");
	}

	@Test
	public void splitsWoodchuckSentence() {
		String sentence = "How much wood would a woodchuck chuck if a woodchuck could chuck wood";
		List<Message<?>> results = this.splitter.apply(new GenericMessage<>(sentence));
		List<String> words = results.stream().map(Message::getPayload).map(Object::toString).toList();
		assertThat(words).containsExactly(sentence.split(" "));
	}

	@SpringBootApplication
	static class SplitterFunctionTestApplication {

	}

}
