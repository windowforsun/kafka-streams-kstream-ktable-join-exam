package com.windowforsun.kafka.streams.join;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

import java.util.List;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = KafkaConfig.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@EmbeddedKafka(controlledShutdown = true, topics = {"view-topic", "click-topic", "result-topic"})
@ActiveProfiles("test")
public class EventStreamsTableJoinTest {
	private StreamsBuilder streamsBuilder;
	private Serde<String> stringSerde = new Serdes.StringSerde();
	private TopologyTestDriver topologyTestDriver;
	private TestInputTopic<String, String> viewEventInput;
	private TestInputTopic<String, String> clickEventInput;
	private TestOutputTopic<String, String> resultOutput;
	@Autowired
	private EmbeddedKafkaBroker embeddedKafkaBroker;
	@Autowired
	private KafkaListenerEndpointRegistry registry;

	@BeforeEach
	public void setUp() {
		this.registry.getListenerContainers()
			.stream()
			.forEach(container -> ContainerTestUtils.waitForAssignment(container,
				this.embeddedKafkaBroker.getPartitionsPerTopic()));

		this.streamsBuilder = new StreamsBuilder();
		EventStreamsTableJoin eventStreamsTableJoin = new EventStreamsTableJoin(10000L,
			0L,
			"click-topic",
			"view-topic",
			"result-topic");
		eventStreamsTableJoin.process(this.streamsBuilder);
		final Topology topology = this.streamsBuilder.build();


		Properties props = new Properties();
		// props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-application");
		// props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234"); // 테스트용으로 dummy 값 사용
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

		this.topologyTestDriver = new TopologyTestDriver(topology, props);
		this.viewEventInput = this.topologyTestDriver.createInputTopic("view-topic",
			this.stringSerde.serializer(),
			this.stringSerde.serializer());
		this.clickEventInput = this.topologyTestDriver.createInputTopic("click-topic",
			this.stringSerde.serializer(),
			this.stringSerde.serializer());
		this.resultOutput = this.topologyTestDriver.createOutputTopic("result-topic",
			this.stringSerde.deserializer(),
			this.stringSerde.deserializer());
	}

	@AfterEach
	public void tearDown() {
		if(this.topologyTestDriver != null) {
			this.topologyTestDriver.close();
		}
	}

	@Test
	public void viewStream_clickTable_join() {
		Util.sendEvent(this.viewEventInput, this.clickEventInput);

		List<TestRecord<String, String>> recordList = this.resultOutput.readRecordsToList();

		System.out.println(recordList.size());
		recordList.forEach(System.out::println);

		assertThat(recordList, hasSize(1));

		assertThat(recordList.get(0).timestamp(), is(3000L));
		assertThat(recordList.get(0).key(), is("C"));
		assertThat(recordList.get(0).value(), is("VIEW:C1, CLICK:C1"));
	}

}
