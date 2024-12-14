package com.windowforsun.kafka.streams.join;

import org.apache.kafka.streams.TestInputTopic;

public class Util {
	public static void sendEvent(TestInputTopic<String, String> viewEventInput, TestInputTopic<String, String> clickEventInput) {
		viewEventInput.pipeInput("A", "VIEW:A1", 0L);
		viewEventInput.pipeInput("B", "VIEW:B1", 1000L);
		clickEventInput.pipeInput("A", "CLICK:A1", 1000L);
		clickEventInput.pipeInput("C", "CLICK:C1", 2000L);
		viewEventInput.pipeInput("C", "VIEW:C1", 3000L);
		viewEventInput.pipeInput("D", "VIEW:D1", 4000L);
		clickEventInput.pipeInput("E", "CLICK:E1", 5000L);
		viewEventInput.pipeInput("F", "VIEW:F1", 6000L);
		viewEventInput.pipeInput("F", "VIEW:F2", 6000L);
		clickEventInput.pipeInput("F", "CLICK:F1", 7000L);
		viewEventInput.pipeInput("G", "VIEW:G1", 8000L);
		clickEventInput.pipeInput("G", "CLICK:G1", 9000L);
		clickEventInput.pipeInput("G", "CLICK:G2", 9000L);
		clickEventInput.pipeInput("B", "CLICK:B1", 12000L);
	}

	public static void sendViewEvent(TestInputTopic<String, String> viewEventInput) {
		viewEventInput.pipeInput("A", "VIEW:A1", 0L);
		viewEventInput.pipeInput("B", "VIEW:B1", 1000L);
		viewEventInput.pipeInput("C", "VIEW:C1", 3000L);
		viewEventInput.pipeInput("D", "VIEW:D1", 4000L);
		viewEventInput.pipeInput("F", "VIEW:F1", 6000L);
		viewEventInput.pipeInput("F", "VIEW:F2", 6000L);
		viewEventInput.pipeInput("G", "VIEW:G1", 8000L);
	}

	public static void sendClickEvent(TestInputTopic<String, String> clickEventInput) {
		clickEventInput.pipeInput("A", "CLICK:A1", 1000L);
		clickEventInput.pipeInput("C", "CLICK:C1", 2000L);
		clickEventInput.pipeInput("E", "CLICK:E1", 5000L);
		clickEventInput.pipeInput("F", "CLICK:F1", 7000L);
		clickEventInput.pipeInput("G", "CLICK:G1", 9000L);
		clickEventInput.pipeInput("G", "CLICK:G2", 9000L);
		clickEventInput.pipeInput("B", "CLICK:B1", 12000L);
	}
}
