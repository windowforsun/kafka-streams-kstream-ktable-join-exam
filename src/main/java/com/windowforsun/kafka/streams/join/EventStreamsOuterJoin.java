package com.windowforsun.kafka.streams.join;

import java.time.Duration;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventStreamsOuterJoin {
	private final Long windowDuration;
	private final Long windowGrace;
	private final String viewTopic;
	private final String clickTopic;
	private final String resultTopic;

	public EventStreamsOuterJoin(Long windowDuration,
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

		KStream<String, String> joinedStream = viewStream.outerJoin(clickStream,
			(leftViewValue, rightClickValue) -> {
				String result = leftViewValue + ", " + rightClickValue;
				log.info(result);
				return result;
			},
			joinWindows,
			StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
		);

		joinedStream.to(this.resultTopic);
	}
}
