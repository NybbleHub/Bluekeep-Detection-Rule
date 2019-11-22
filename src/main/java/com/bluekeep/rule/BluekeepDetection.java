package com.bluekeep.rule;

import com.bluekeep.rule.markovchain.MarkovChainTraining;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;
import java.util.List;


public class BluekeepDetection {

	private static MarkovChainTraining markovChain = new MarkovChainTraining();
	private static TimeZone tz = TimeZone.getTimeZone("UTC");
	private static DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

	public static void main(String[] args) throws Exception {

		df.setTimeZone(tz);

		//Train Markov Chain
		markovChain.trainModel();

		// Set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// Set up Kafka environment
		Properties kafkaProperties = new Properties();
		kafkaProperties.setProperty("bootstrap.servers", "127.0.0.1:9092");
		kafkaProperties.setProperty("group.id", "flink_kafka_consumer");

		// Set up ElasticSearch environment
		List<HttpHost> httpHosts = new ArrayList<>();
		httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));


		// Create a Kafka consumer where topic is "windows-logs", using JSON Deserialization schema and properties provided above. Read from the beginning.
		JSONKeyValueDeserializationSchema logsSchema = new JSONKeyValueDeserializationSchema(false);
		FlinkKafkaConsumer<ObjectNode> windowsLogsConsumer = new FlinkKafkaConsumer("windows-logs", logsSchema, kafkaProperties);
		windowsLogsConsumer.setStartFromEarliest();

		// Create a ElasticSearch sink where index is "alert_index"
		ElasticsearchSink.Builder<String> esSinkAlertBuilder = new ElasticsearchSink.Builder<>(httpHosts, new ElasticsearchSinkFunction<String>() {
			public IndexRequest createIndexRequest(String element) throws IOException {
				ObjectMapper mapper = new ObjectMapper();
				HashMap json = mapper.readValue(element, HashMap.class);

				return Requests.indexRequest()
						.index("alert_index")
						.type("alert")
						.source(json);
			}

			@Override
			public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
				try {
					indexer.add(createIndexRequest(element));
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});

		// Create a ElasticSearch sink where index is "bluekeep"
		ElasticsearchSink.Builder<String> esSinkDataBuilder = new ElasticsearchSink.Builder<>(httpHosts, new ElasticsearchSinkFunction<String>() {
			public IndexRequest createIndexRequest(String element) throws IOException {
				ObjectMapper mapper = new ObjectMapper();
				HashMap json = mapper.readValue(element, HashMap.class);

				return Requests.indexRequest()
						.index("bluekeep")
						.type("_doc")
						.source(json);
			}

			@Override
			public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
				try {
					indexer.add(createIndexRequest(element));
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});

		// Configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
		esSinkAlertBuilder.setBulkFlushMaxActions(1);
		esSinkDataBuilder.setBulkFlushMaxActions(1);


		DataStream<ObjectNode> windowsBluekeepStream = env.addSource(windowsLogsConsumer).filter(new FilterFunction<ObjectNode>() {
			@Override
			public boolean filter(ObjectNode jsonNodes) throws Exception {
				Integer eventId = jsonNodes.get("value").get("winlog").get("event_id").asInt();
				return (eventId == 1149 || eventId == 3 || eventId == 1);
			}
		}).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ObjectNode>() {
			@Override
			public long extractAscendingTimestamp(ObjectNode jsonNodes) {
				// Keep event order following date in "Event.Created" field.
				Long eventCreateTimeStamp = Instant.parse(jsonNodes.get("value").get("event").get("created").asText()).toEpochMilli();
				return eventCreateTimeStamp;
			}
		});

		// Send Events to Elasticsearch
		windowsBluekeepStream.map(ObjectNode::toString).addSink(esSinkDataBuilder.build());

		Pattern<ObjectNode, ?> bluekeepPattern = Pattern.<ObjectNode>begin("Bluekeep Scan EID 1149").where(
				new IterativeCondition<ObjectNode>() {
					@Override
					public boolean filter(ObjectNode jsonNodes, Context<ObjectNode> context) throws Exception {
						if (jsonNodes.get("value").get("winlog").get("event_id").asInt() != 1149) {
							return false;
						}
						// Extract Username for Event and check if it may have been randomly generated
						String username = jsonNodes.get("value").get("winlog").get("user_data").get("Param1").asText();
						Boolean usernameIsRandom = markovChain.isRandom(username);

						return usernameIsRandom;
					}
				}
		).followedBy("Bluekeep Scan EID 3").where(
				new IterativeCondition<ObjectNode>() {
					@Override
					public boolean filter(ObjectNode jsonNodes, Context<ObjectNode> context) throws Exception {
						String sourceIpEID1149 = null;

						// Relaxed Contiguity : event with EventID 1149 is followed by an event with EventID 3
						// but maybe with some other events between them.
						if (jsonNodes.get("value").get("winlog").get("event_id").asInt() != 3) {
							return false;
						}

						// Retrieve Param3 value from event from previous pattern "Bluekeep Scan EID 1149
						// Param3 in EventID 1149 corresponds to Source IP Address
						for (ObjectNode bluekeepEID1149 : context.getEventsForPattern("Bluekeep Scan EID 1149")) {
							sourceIpEID1149 = bluekeepEID1149.get("value").get("winlog").get("user_data").get("Param3").asText();
						}

						// Return false if Param3 from event from Previous pattern is null
						if (sourceIpEID1149 == null) {
							return false;
						}
						// Get event if SourceIP in previous EventID 1149 equal SourceIP in Sysmon Event 3
						return  sourceIpEID1149.equals(jsonNodes.get("value").get("source").get("ip").asText());
					}
				}
		).followedBy("Bluekeep Scan EID 3 Initiated").where(
				new IterativeCondition<ObjectNode>() {
					@Override
					public boolean filter(ObjectNode jsonNodes, Context<ObjectNode> context) throws Exception {
						String sourceIpEID3 = null;

						// Relaxed Contiguity : event with EventID 3 is followed by an event with EventID 3
						// but maybe with some other events between them.
						if (jsonNodes.get("value").get("winlog").get("event_id").asInt() != 3) {
							return false;
						}

						for (ObjectNode bluekeepEID3 : context.getEventsForPattern("Bluekeep Scan EID 3")) {
							sourceIpEID3 = bluekeepEID3.get("value").get("source").get("ip").asText();
						}

						if (sourceIpEID3 == null) {
							return false;
						}

						// Get Event if SourceIP from previous Sysmon EventID 3 equal DestinationIP in Sysmon EventID 3
						// In this case source of scan probably successfully exploit the target because "Initiated" equal "true"

						return  sourceIpEID3.equals(jsonNodes.get("value").get("destination").get("ip").asText())
								&& jsonNodes.get("value").get("network").get("direction").asText().equals("outbound");
					}
				}
		).within(Time.seconds(10));

		PatternStream<ObjectNode> bluekeepPatternStream = CEP.pattern(windowsBluekeepStream, bluekeepPattern);

		DataStream<ObjectNode> bluekeepDetected = bluekeepPatternStream.select(
				new PatternSelectFunction<ObjectNode, ObjectNode>() {
					@Override
					public ObjectNode select(Map<String, List<ObjectNode>> map) throws Exception {
						ObjectMapper mapper = new ObjectMapper();
						ObjectNode bluekeepScanAlert = mapper.createObjectNode();

						List<ObjectNode> eventNodeList = new ArrayList<>();
						List<ObjectNode> eventIDNodeList = new ArrayList<>();
						List<ObjectNode> eventPIDNodeList = new ArrayList<>();
						List<ObjectNode> eventPExecNodeList = new ArrayList<>();
						List<ObjectNode> eventPNameNodeList = new ArrayList<>();

						eventNodeList.add(map.get("Bluekeep Scan EID 1149").get(0).get("value").deepCopy());
						eventNodeList.add(map.get("Bluekeep Scan EID 3").get(0).get("value").deepCopy());
						eventNodeList.add(map.get("Bluekeep Scan EID 3 Initiated").get(0).get("value").deepCopy());

						eventIDNodeList.add(map.get("Bluekeep Scan EID 1149").get(0).get("value").get("winlog").get("event_id").deepCopy());
						eventIDNodeList.add(map.get("Bluekeep Scan EID 3").get(0).get("value").get("winlog").get("event_id").deepCopy());
						eventIDNodeList.add(map.get("Bluekeep Scan EID 3 Initiated").get(0).get("value").get("winlog").get("event_id").deepCopy());

						eventPIDNodeList.add(map.get("Bluekeep Scan EID 1149").get(0).get("value").get("winlog").get("process").get("pid").deepCopy());
						eventPIDNodeList.add(map.get("Bluekeep Scan EID 3").get(0).get("value").get("winlog").get("process").get("pid").deepCopy());
						eventPIDNodeList.add(map.get("Bluekeep Scan EID 3 Initiated").get(0).get("value").get("winlog").get("process").get("pid").deepCopy());

						eventPExecNodeList.add(map.get("Bluekeep Scan EID 3").get(0).get("value").get("process").get("executable").deepCopy());
						eventPExecNodeList.add(map.get("Bluekeep Scan EID 3 Initiated").get(0).get("value").get("process").get("executable").deepCopy());

						eventPNameNodeList.add(map.get("Bluekeep Scan EID 3").get(0).get("value").get("process").get("name").deepCopy());
						eventPNameNodeList.add(map.get("Bluekeep Scan EID 3 Initiated").get(0).get("value").get("process").get("name").deepCopy());

						bluekeepScanAlert.put("message", "A Bluekeep scan followed by a connection to the source of scan has been detected. " +
								"This can indicate a successfull exploit of CVE 2019-0708.");

						bluekeepScanAlert.set("destination.ip", map.get("Bluekeep Scan EID 3").get(0)
								.get("value").get("destination").get("ip").deepCopy());

						bluekeepScanAlert.set("destination.port", map.get("Bluekeep Scan EID 3").get(0)
								.get("value").get("destination").get("port").deepCopy());

						bluekeepScanAlert.set("source.ip", map.get("Bluekeep Scan EID 3").get(0)
								.get("value").get("source").get("ip").deepCopy());

						bluekeepScanAlert.set("host.hostname", map.get("Bluekeep Scan EID 3").get(0)
								.get("value").get("host").get("hostname").deepCopy());

						bluekeepScanAlert.set("host.os.family", map.get("Bluekeep Scan EID 3").get(0)
								.get("value").get("host").get("os").get("family").deepCopy());

						bluekeepScanAlert.set("host.os.full", map.get("Bluekeep Scan EID 3").get(0)
								.get("value").get("host").get("os").get("name").deepCopy());

						bluekeepScanAlert.set("host.os.full", map.get("Bluekeep Scan EID 3").get(0)
								.get("value").get("host").get("os").get("name").deepCopy());

						bluekeepScanAlert.set("destination.user.name", map.get("Bluekeep Scan EID 3").get(0)
								.get("value").get("user").get("name").deepCopy());

						bluekeepScanAlert.set("destination.user.domain", map.get("Bluekeep Scan EID 3").get(0)
								.get("value").get("user").get("domain").deepCopy());

						bluekeepScanAlert.put("alert.processing.time", df.format(new Date()));

						bluekeepScanAlert.put("threat.framework", "MITRE ATT&CK");

						bluekeepScanAlert.put("threat.tactic.id", "TA0040");

						bluekeepScanAlert.put("threat.tactic.name", "lateral movement");

						bluekeepScanAlert.put("threat.tactic.reference", "https://attack.mitre.org/tactics/TA0040/");

						bluekeepScanAlert.put("threat.technique.id", "T1210");

						bluekeepScanAlert.put("threat.technique.name", "exploitation of remote services");

						bluekeepScanAlert.put("threat.technique.reference", "https://attack.mitre.org/techniques/T1210/");

						// Retrieve EventID list in order
						ArrayNode eventIDList = mapper.valueToTree(eventIDNodeList);
						bluekeepScanAlert.putArray("event.code").addAll(eventIDList);

						// Retrieve PID list in order
						ArrayNode eventPIDList = mapper.valueToTree(eventPIDNodeList);
						bluekeepScanAlert.putArray("process.pid").addAll(eventPIDList);

						// Retrieve absolute path to the process executable list in order
						ArrayNode eventPExecList = mapper.valueToTree(eventPExecNodeList);
						bluekeepScanAlert.putArray("process.executable").addAll(eventPExecList);

						// Retrieve process name list in order
						ArrayNode eventPNameList = mapper.valueToTree(eventPNameNodeList);
						bluekeepScanAlert.putArray("process.name").addAll(eventPNameList);

						// Retrieve original events for investigation
						ArrayNode eventOriginal = mapper.valueToTree(eventNodeList);
						bluekeepScanAlert.putArray("event.original").addAll(eventOriginal);

						System.out.println("Possible Bluekeep scan and exploit detected");

						return bluekeepScanAlert;
					}
				}
		);

		// Send Alerts to Elasticsearch
		bluekeepDetected.map(ObjectNode::toString).addSink(esSinkAlertBuilder.build());

		bluekeepDetected.print();

		// execute program
		env.execute("Flink Streaming Bluekeep detection");
	}
}
