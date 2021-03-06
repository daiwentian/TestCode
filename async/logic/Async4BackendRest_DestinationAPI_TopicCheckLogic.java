package com.open.jp.async.logic;

import static java.util.Collections.singleton;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

import javax.ejb.Asynchronous;
import javax.ejb.Stateless;
import javax.ws.rs.core.Response;

import com.open.jp.async.errorresponse.ErrorResponse;
import com.open.jp.async.response.Responses;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.json.JSONObject;


@Stateless
public class Async4BackendRest_DestinationAPI_TopicCheckLogic {
	
	private static final String CONSUMER_GROUP_ID = "Async4BackendRest_Group";
	private static final String OUTPUT_TOPIC = "Async4BackendRest_CallbackAPI_Topic";
	private static final String INPUT_TOPIC = "Async4BackendRest_DestinationAPI_Topic";
	

	public Async4BackendRest_DestinationAPI_TopicCheckLogic() {
		
	}
	
	/**
	 * Description<br>
	 * ?????????????????????????????????????????????????????????.<br>
	 * 
	 * ErrorResponseEnum???????????????????????????ErrorStatusCodeMessage???ErrorMessage???ErrorStatus???ErrorResponse????????????
	 * Time???Code???Message?????????????????????????????????????????????????????????????????????????????????????????????.<br>
	 * 
	 * @version 2.0
	 * @param errorresponseenum "E400" "E409" "E00" ErrorResponseEnum??????????????????????????????????????????????????????.
	 * @return ?????????????????????"ErrorResponseEnum"?????????????????????????????????????????????????????????????????????????????????
	 */
	public  Response GeneralErrorRes(ErrorResponseEnum errorresponseenum) {
	    ErrorResponse erroresponse = new ErrorResponse();
	    erroresponse.setTime(LocalDateTime.now().toString()); 
	    erroresponse.setCode(errorresponseenum.getErrorStatusCodeMessage()); 
	    erroresponse.setMessage(errorresponseenum.getErrorMessage()); 
	    return Response.status(errorresponseenum.getErrorStatus()).entity(erroresponse).build();
	}
	
	@Asynchronous
	public Future<Response> ExecThreadAsync4BackendRest_DestinationAPI() {

		KafkaConsumer<String, String> consumer = createKafkaConsumer();
		KafkaProducer<String, String> producer = createKafkaProducer();

		while(true) {

			producer.initTransactions();

			try {
				while (true) {
					Response typeresponse = ExecDestinationAPIMain(consumer, producer);
					

					//return;
				}

			} catch (KafkaException e) {
				
				producer.abortTransaction();

			}

		}
	}

	private static KafkaConsumer<String, String> createKafkaConsumer() {
    	Properties props = new Properties();
    	props.put(BOOTSTRAP_SERVERS_CONFIG, "asynckafka1:9092");
    	props.put(GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
    	props.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
    	props.put(ISOLATION_LEVEL_CONFIG, "read_committed");
		props.put(MAX_POLL_RECORDS_CONFIG, 1);
        props.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
    	props.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    	props.put(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

    	KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    	consumer.subscribe(singleton(INPUT_TOPIC));
    	return consumer;
	}

	private static KafkaProducer<String, String> createKafkaProducer() {

    	Properties props = new Properties();
    	props.put(BOOTSTRAP_SERVERS_CONFIG, "asynckafka1:9092");
    	props.put(ENABLE_IDEMPOTENCE_CONFIG, "true");
    	props.put(TRANSACTIONAL_ID_CONFIG, "prod-1");
    	props.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    	props.put(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

    	return new KafkaProducer(props);

	}
	
	private Response ExecDestinationAPIMain(KafkaConsumer<String, String> consumer, KafkaProducer<String, String> producer) {

		StringBuilder destinationStatus = new StringBuilder();
		StringBuilder destinationResponseBody = new StringBuilder();
		String receiveMessage = new String();
		StringBuilder executionTimes = new StringBuilder();
		StringBuilder sendMessage = new StringBuilder();
		StringBuilder retryMessage = new StringBuilder();
		Responses response = new Responses();

		receiveMessage = null;
		destinationStatus.delete(0, destinationStatus.length());
		destinationResponseBody.delete(0, destinationResponseBody.length());
		executionTimes.delete(0, executionTimes.length());
		sendMessage.delete(0, sendMessage.length());
		retryMessage.delete(0, retryMessage.length());
		consumer.subscribe(Arrays.asList(INPUT_TOPIC));

		// ??????????????????Topic1????????????
		ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(60l));

		for (ConsumerRecord<String, String> record : records) {
			System.out.println(String.format("*record* %s:%s", record.offset(), record.value()));
			receiveMessage = record.value();
		}

		// ???????????????????????????????????????
		if(receiveMessage == null) {
			System.out.println("Async4BackendRest_DestinationAPI_Topic relode");
			//continue;
			return Response.status(Response.Status.CREATED.getStatusCode()).entity(response).build();
					}

		Response typeresponse = ExecuteDestinationAPI(receiveMessage, destinationStatus, destinationResponseBody, executionTimes);

		// ??????????????????API?????????????????????
		if (Integer.parseInt(destinationStatus.toString()) >= Response.Status.MOVED_PERMANENTLY.getStatusCode()) {
			System.out.println("Destination API Error.");
			// ???????????????????????????
			if(3 < Integer.parseInt(executionTimes.toString())) {
				// ??????
				System.out.println("over 3times");

			} else {
				// ????????????????????????(???????????????1????????????)
				typeresponse = CreateRetryMessage(receiveMessage, retryMessage, executionTimes);
					
				producer.beginTransaction();
				
				// Topic???????????????
				producer.send(new ProducerRecord<String, String>(INPUT_TOPIC, retryMessage.toString()));
							
				Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();

				for (TopicPartition partition : records.partitions()) {
					List<ConsumerRecord<String, String>> partitionedRecords = records.records(partition);
					long offset = partitionedRecords.get(partitionedRecords.size() - 1).offset();

					offsetsToCommit.put(partition, new OffsetAndMetadata(offset + 1));
				}

				producer.sendOffsetsToTransaction(offsetsToCommit, CONSUMER_GROUP_ID);

				// ??????????????????????????????
				producer.commitTransaction();
							
				//continue;
				return Response.status(Response.Status.CREATED.getStatusCode()).entity(response).build();
			}
		}
		
		// ???????????????????????????
		typeresponse = CreateSendMessage(receiveMessage, sendMessage, destinationStatus, destinationResponseBody);
		
		producer.beginTransaction();
		
		// ??????????????????Topic2?????????
		producer.send(new ProducerRecord<String, String>(OUTPUT_TOPIC, sendMessage.toString()));
		
		Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();

		for (TopicPartition partition : records.partitions()) {
			List<ConsumerRecord<String, String>> partitionedRecords = records.records(partition);
			long offset = partitionedRecords.get(partitionedRecords.size() - 1).offset();

			offsetsToCommit.put(partition, new OffsetAndMetadata(offset + 1));
		}
		
		producer.sendOffsetsToTransaction(offsetsToCommit, CONSUMER_GROUP_ID);
		
		// ??????????????????????????????
		producer.commitTransaction();
		

		return Response.status(Response.Status.CREATED.getStatusCode()).entity(response).build();
	}

	private Response ExecuteDestinationAPI(String receivemessage, StringBuilder destinationStatus, StringBuilder destinationResponseBody, StringBuilder executionTimes) {
	    
	    Responses response = new Responses();
	    
	    String destinationUri = new String();
	    String destinationHttpMethod = new String();
	    String destinatioRrequestHeader = new String();
		String destinatioRrequestHeaderKey = new String();
		String destinatioRrequestHeaderValue = new String();
	    String destinationRequestBody = new String();
	    
        try {
        	destinationStatus.delete(0, destinationStatus.length());
        	destinationResponseBody.delete(0, destinationResponseBody.length());
			executionTimes.delete(0, executionTimes.length());
			
        	JSONObject jsonObject = new JSONObject(receivemessage);

			// URLDecode??????
			destinationUri = URLDecoder.decode(jsonObject.getString("destinationuri"), "utf-8");
			
			destinationHttpMethod = jsonObject.getString("destinationhttpmethod");
			
			if(receivemessage.contains("destinationrequestheader")) {
				destinatioRrequestHeader = jsonObject.getString("destinationrequestheader");
				
				String[] splitresult = destinatioRrequestHeader.split(":", 0);
				destinatioRrequestHeaderKey = splitresult[0];
				
				destinatioRrequestHeaderValue = splitresult[1];
				
			}
			
			if(receivemessage.contains("destinationrequestbody")) {
				// URLDecode??????
				destinationRequestBody = URLDecoder.decode(jsonObject.getString("destinationrequestbody"), "utf-8");
				
			}
			
			executionTimes.append(jsonObject.getString("executiontimes"));
			
			URL url = new URL(destinationUri);
			
			HttpURLConnection urlConnection = null;
			
			urlConnection = (HttpURLConnection) url.openConnection();
			
			urlConnection.setRequestMethod(destinationHttpMethod);
			
			urlConnection.setUseCaches(false);
			
			urlConnection.setDoOutput(true);
			
			if(receivemessage.contains("destinationrequestheader")) {
				urlConnection.setRequestProperty(destinatioRrequestHeaderKey, destinatioRrequestHeaderValue);
			}
			
			OutputStreamWriter out = new OutputStreamWriter(
					new BufferedOutputStream(urlConnection.getOutputStream()));
			
			if(receivemessage.contains("destinationrequestbody")) {
				out.write(destinationRequestBody.toString());
			}
			
			out.close();
			
			destinationStatus.append(urlConnection.getResponseCode());
			
			BufferedReader br = null;
			
			try {
				// ??????????????????????????????
				br = new BufferedReader(new InputStreamReader(urlConnection.getInputStream(), "UTF-8"));
				
				String line;
				
				while((line = br.readLine()) != null) {
					destinationResponseBody.append(line);
				}
				
				br.close();
				
			// 200?????????????????????
			} catch(IOException e) {
				
				BufferedReader errbr = null;
				try {
					
					errbr = new BufferedReader(new InputStreamReader(urlConnection.getErrorStream(), "UTF-8"));

					String errline;
					
					while((errline = errbr.readLine()) != null) {
						destinationResponseBody.append(errline);
					}
					
					return GeneralErrorRes(ErrorResponseEnum.E500);
				} catch(Exception e1) {
					
					e1.printStackTrace();
				} finally {
					if(errbr != null) {
						try {
							errbr.close();
						} catch (IOException e2) {
							
							e2.printStackTrace();
						}
					}
				}
			} catch(Exception e) {
				
				e.printStackTrace();
			} finally {
		    	if (br != null) {
		    		br.close();
		    	}
		    }
		} catch(Exception e) {
			
        	e.printStackTrace();
			
	    	return GeneralErrorRes(ErrorResponseEnum.E500);
	    } 
        	    
	    return Response.status(Response.Status.CREATED.getStatusCode()).entity(response).build();
	}

	private Response CreateRetryMessage(String receivemessage, StringBuilder retrymessage,  StringBuilder executiontimes) {
    
		Responses response = new Responses();
		
		String transactionId = new String();
		String destinationUri = new String();
		String destinationHttpMethod = new String();
		String destinatioRrequestHeader = new String();
		String destinationRequestBody = new String();
		String asyncId = new String();
		String callbackUri = new String();
		int retryTime = 1;
		
		try {
			retrymessage.delete(0, retrymessage.length());
			
			JSONObject jsonObject = new JSONObject(receivemessage);
	
	
			transactionId = jsonObject.getString("transactionid");
			
			destinationUri = jsonObject.getString("destinationuri");
			
			destinationHttpMethod = jsonObject.getString("destinationhttpmethod");
			
			asyncId = jsonObject.getString("asyncid");
			
			callbackUri = jsonObject.getString("callbackuri");
			
			retryTime = Integer.parseInt(jsonObject.getString("executiontimes").toString()) + 1;
			
	
			retrymessage.append("{");
			retrymessage.append("\"transactionid\"");
			retrymessage.append(":");
			retrymessage.append("\"");
			retrymessage.append(transactionId);
			retrymessage.append("\"");
			retrymessage.append(",");
			retrymessage.append("\"destinationuri\"");
			retrymessage.append(":");
			retrymessage.append("\"");
			retrymessage.append(destinationUri);
			retrymessage.append("\"");
			retrymessage.append(",");
			retrymessage.append("\"destinationhttpmethod\"");
			retrymessage.append(":");
			retrymessage.append("\"");
			retrymessage.append(destinationHttpMethod);
			retrymessage.append("\"");
	
			if(receivemessage.contains("destinationrequestheader")) {
				destinatioRrequestHeader = jsonObject.getString("destinationrequestheader");
				
				retrymessage.append(",");
				retrymessage.append("\"destinationrequestheader\"");
				retrymessage.append(":");
				retrymessage.append("\"");
				retrymessage.append(destinatioRrequestHeader);
				retrymessage.append("\"");
			}
			
	
			if(receivemessage.contains("destinationrequestbody")) {
				destinationRequestBody = jsonObject.getString("destinationrequestbody");
				
				retrymessage.append(",");
				retrymessage.append("\"destinationrequestbody\"");
				retrymessage.append(":");
				retrymessage.append("\"");
				retrymessage.append(destinationRequestBody);
				retrymessage.append("\"");
			}
			
			retrymessage.append(",");
			retrymessage.append("\"callbackuri\"");
			retrymessage.append(":");
			retrymessage.append("\"");
			retrymessage.append(callbackUri);
			retrymessage.append("\"");
			retrymessage.append(",");
			retrymessage.append("\"asyncid\"");
			retrymessage.append(":");
			retrymessage.append("\"");
			retrymessage.append(asyncId);
			retrymessage.append("\"");
	
			retrymessage.append(",");
			retrymessage.append("\"executiontimes\"");
			retrymessage.append(":");
			retrymessage.append("\"");
			retrymessage.append(Integer.toString(retryTime));
			retrymessage.append("\"");
			retrymessage.append("}");
			
		} catch(Exception e) {
			e.printStackTrace();
			return GeneralErrorRes(ErrorResponseEnum.E500);
		} 
				
		return Response.status(Response.Status.CREATED.getStatusCode()).entity(response).build();
	}
	
	private Response CreateSendMessage(String receivemessage, StringBuilder sendmessage, StringBuilder destinationStatus, StringBuilder destinationResponseBody) {
    
		Responses response = new Responses();
		
		
		String callbackUri = new String();
		String transactionId = new String();
		String asyncId = new String();
		
		try {
			sendmessage.delete(0, sendmessage.length());
			
			JSONObject jsonObject = new JSONObject(receivemessage);
	
			
			callbackUri = jsonObject.getString("callbackuri");
			
			transactionId = jsonObject.getString("transactionid");
			
			asyncId = jsonObject.getString("asyncid");
			
			sendmessage.append("{");
			sendmessage.append("\"destinationresult\"");
			sendmessage.append(":");
			sendmessage.append("\"");
			sendmessage.append("success");
			sendmessage.append("\"");
			sendmessage.append(",");
			sendmessage.append("\"destinationstatuscode\"");
			sendmessage.append(":");
			sendmessage.append("\"");
			sendmessage.append(destinationStatus.toString());
			sendmessage.append("\"");
			
			if(destinationResponseBody != null) {
				sendmessage.append(",");
				sendmessage.append("\"destinationresponsebody\"");
				sendmessage.append(":");
				sendmessage.append("\"");
				sendmessage.append(destinationResponseBody.toString());
				sendmessage.append("\"");
			}
			
			sendmessage.append(",");
			sendmessage.append("\"transactionid\"");
			sendmessage.append(":");
			sendmessage.append("\"");
			sendmessage.append(transactionId);
			sendmessage.append("\"");
			sendmessage.append(",");
			sendmessage.append("\"asyncid\"");
			sendmessage.append(":");
			sendmessage.append("\"");
			sendmessage.append(asyncId);
			sendmessage.append("\"");
			sendmessage.append(",");
			sendmessage.append("\"callbackuri\"");
			sendmessage.append(":");
			sendmessage.append("\"");
			sendmessage.append(callbackUri);
			sendmessage.append("\"");
			sendmessage.append(",");
			sendmessage.append("\"executiontimes\"");
			sendmessage.append(":");
			sendmessage.append("\"");
			sendmessage.append("1");
			sendmessage.append("\"");
			sendmessage.append("}");
			
		} catch(Exception e) {
			e.printStackTrace();
			return GeneralErrorRes(ErrorResponseEnum.E500);
		} 
				
		return Response.status(Response.Status.CREATED.getStatusCode()).entity(response).build();
	}
}

