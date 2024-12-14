package com.windowforsun.kafka.streams.join;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventTableOuterJoin {
	private final Long windowDuration;
	private final Long windowGrace;
	private final String viewTopic;
	private final String clickTopic;
	private final String resultTopic;

	public EventTableOuterJoin(
		Long windowDuration,
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
		KTable<String, String> viewTable = streamsBuilder.table(this.viewTopic, Materialized.as("view-store"));
		KTable<String, String> clickTable = streamsBuilder.table(this.clickTopic, Materialized.as("click-store"));

		KTable<String, String> joinTable = viewTable.outerJoin(clickTable,
			(leftViewValue, rightClickValue) -> {
				String result = leftViewValue + ", " + rightClickValue;
				log.info(result);
				return result;
			});

		joinTable.toStream().to(this.resultTopic, Produced.with(Serdes.String(), Serdes.String()));
	}
}
