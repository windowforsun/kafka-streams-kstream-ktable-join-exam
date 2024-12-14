package com.windowforsun.kafka.streams.join;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import com.windowforsun.kafka.streams.join.KafkaConfig;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, classes = KafkaConfig.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@EmbeddedKafka(controlledShutdown = true, topics = {"my-event", "hopping-result"})
@ActiveProfiles("test")
public class ExamTest {

}
