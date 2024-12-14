package com.windowforsun.kafka.streams.join;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class EventTableJoin {
	private final Long windowDuration;
	private final Long windowGrace;
	private final String viewTopic;
	private final String clickTopic;
	private final String resultTopic;

	public EventTableJoin(@Value("${app.window.duration}") Long windowDuration,
		@Value("${app.window.grace}") Long windowGrace,
		@Value("${app.topic.table.click}") String clickTopic,
		@Value("${app.topic.table.view}") String viewTopic,
		@Value("${app.topic.result}") String resultTopic) {
		this.windowDuration = windowDuration;
		this.windowGrace = windowGrace;
		this.viewTopic = viewTopic;
		this.clickTopic = clickTopic;
		this.resultTopic = resultTopic;
	}
	// public EventStreamsJoin(Long windowDuration,
	// 	Long windowGrace,
	// 	String clickTopic,
	// 	String viewTopic,
	// 	String resultTopic) {
	// 	this.windowDuration = windowDuration;
	// 	this.windowGrace = windowGrace;
	// 	this.viewTopic = viewTopic;
	// 	this.clickTopic = clickTopic;
	// 	this.resultTopic = resultTopic;
	// }


	@Autowired
	public void process(StreamsBuilder streamsBuilder) {
		KTable<String, String> viewTable = streamsBuilder.table(this.viewTopic, Materialized.as("view-store"));
		KTable<String, String> clickTable = streamsBuilder.table(this.clickTopic, Materialized.as("click-store"));

		KTable<String, String> joinTable = viewTable.join(clickTable,
			(leftViewValue, rightClickValue) -> {
				String result = leftViewValue + ", " + rightClickValue;
				log.info(result);
				return result;
			});

		joinTable.toStream().to(this.resultTopic, Produced.with(Serdes.String(), Serdes.String()));
	}
}
