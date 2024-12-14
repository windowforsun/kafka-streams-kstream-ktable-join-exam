package com.windowforsun.kafka.streams.join;

import java.time.Duration;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.internals.ValueAndTimestampSerde;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventStreamsJoin2 {
	private final Long windowDuration;
	private final Long windowGrace;
	private final String viewTopic;
	private final String clickTopic;
	private final String resultTopic;

	public EventStreamsJoin2(Long windowDuration,
		Long windowGrace,
		String clickTopic,
		String viewTopic,
		String resultTopic) {
		this.windowDuration = windowDuration;
		this.windowGrace = windowGrace;
		this.viewTopic = viewTopic;
		this.clickTopic = clickTopic;
		this.resultTopic = resultTopic;
	}

	public void process(StreamsBuilder streamsBuilder) {
		KStream<String, String> viewStream = streamsBuilder.stream(this.viewTopic);
		KStream<String, String> clickStream = streamsBuilder.stream(this.clickTopic);
		JoinWindows joinWindows = JoinWindows.of(Duration.ofMillis(this.windowDuration));

		KStream<String, String> joinedStream = clickStream.leftJoin(viewStream,
			(leftClickValue, rightViewValue) -> {
				String result = rightViewValue + ", " + leftClickValue;
				log.info(result);
				return result;
			},
			joinWindows,
			StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
		);

		streamsBuilder.addStateStore(Stores.keyValueStoreBuilder(
			Stores.persistentKeyValueStore("buffer-store"),
			Serdes.String(),
			new ValueAndTimestampSerde<>(Serdes.String()))
		);

		KStream<String, String> filteredStream = joinedStream.transform(() -> new Transformer<String, String, KeyValue<String, String>>() {
			private KeyValueStore<String, ValueAndTimestamp<String>> bufferStore;

			@Override
			public void init(ProcessorContext processorContext) {
				this.bufferStore = (KeyValueStore<String, ValueAndTimestamp<String>>) processorContext.getStateStore("buffer-store");
			}

			@Override
			public KeyValue<String, String> transform(String key, String value) {
				if (value.contains("null")) {
					this.bufferStore.put(key, ValueAndTimestamp.make(value, System.currentTimeMillis()));

					return null;
				} else {
					ValueAndTimestamp<String> bufferedValue = this.bufferStore.get(key);

					if (bufferedValue != null) {
						bufferStore.delete(key);

						return null;
					} else {
						return new KeyValue<>(key, value);
					}

				}
			}

			@Override
			public void close() {

			}
		}, "buffer-store");

		filteredStream.to(this.resultTopic);
	}
}
