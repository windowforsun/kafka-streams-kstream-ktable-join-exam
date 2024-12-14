package com.windowforsun.kafka.streams.join;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventStreamsGlobalTableJoin {
	private final Long windowDuration;
	private final Long windowGrace;
	private final String viewTopic;
	private final String clickTopic;
	private final String resultTopic;

	public EventStreamsGlobalTableJoin(Long windowDuration,
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
		GlobalKTable<String, String> clickGlobalTable = streamsBuilder.globalTable(this.clickTopic, Materialized.as("click-store"));

		KStream<String, String> joinedStream = viewStream.join(clickGlobalTable,
			(leftViewKey, rightClickKey) -> leftViewKey,
			(leftViewValue, rightClickValue) -> {
				String result = leftViewValue + ", " + rightClickValue;
				log.info(result);
				return result;
			});

		joinedStream.to(this.resultTopic, Produced.with(Serdes.String(), Serdes.String()));
	}
}
