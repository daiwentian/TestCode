package com.open.jp.async.logic;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.ejb.AsyncResult;
import javax.ejb.Asynchronous;
import javax.ejb.Stateless;
import javax.ejb.TransactionManagement;
import javax.ejb.TransactionManagementType;
import javax.ws.rs.core.Response;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import com.open.jp.async.errorresponse.ErrorResponse;
import com.open.jp.async.response.Responses;


@Stateless
@TransactionManagement(TransactionManagementType.BEAN)
public class Async4BackendRest_CallbackAPI_TopicCheckLogic {
	
	public Async4BackendRest_CallbackAPI_TopicCheckLogic() {
		
	}
	public  Response GeneralErrorRes(ErrorResponseEnum errorresponseenum) {
	    ErrorResponse erroresponse = new ErrorResponse();
	    erroresponse.setTime(LocalDateTime.now().toString()); 
	    erroresponse.setCode(errorresponseenum.getErrorStatusCodeMessage()); 
	    erroresponse.setMessage(errorresponseenum.getErrorMessage()); 
	    return Response.status(errorresponseenum.getErrorStatus()).entity(erroresponse).build();
	}
	
	

	@Asynchronous
	public Future<Response> ExecThreadAsync4BackendRest_CallbackAPI() {
		
		Responses response = new Responses();
		StringBuilder destinationStatusCode = new StringBuilder();
		StringBuilder destinationResponseBody = new StringBuilder();
		StringBuilder callbackUri = new StringBuilder();
		StringBuilder destinationresult = new StringBuilder();
		StringBuilder transactionid = new StringBuilder();
		StringBuilder asyncid = new StringBuilder();
		StringBuilder executiontime = new StringBuilder();

		
		while(true) {
			System.out.println("PickupMessageForResponseQueue");
			Response typeresponse = PickupMessageForResponseQueue(destinationStatusCode, destinationResponseBody, callbackUri, destinationresult, transactionid, asyncid, executiontime);
		
			if (((Response) typeresponse).getStatus() != Response.Status.CREATED.getStatusCode()) {
				return new AsyncResult<Response>(typeresponse);

			}else{
				
			}

			if(destinationStatusCode.length() == 0 || destinationResponseBody.length() == 0 || callbackUri.length() == 0 || destinationresult.length() == 0 || destinationresult.length() == 0 || transactionid.length() == 0 || asyncid.length() == 0 || executiontime.length() == 0) {
				System.out.println("Async4BackendRest_CallbackAPI_Topic redo");
				continue;
			}
			
			System.out.println("Async4BackendRest_CallbackAPI_Topic messages picked");
			System.out.println("Test1");

			typeresponse = ExecuteCallBackAPI(callbackUri, destinationStatusCode, destinationResponseBody, transactionid, asyncid, executiontime, destinationresult);
			System.out.println("Test2");
			if (((Response) typeresponse).getStatus() != Response.Status.CREATED.getStatusCode()) {
				new AsyncResult<Response>(typeresponse);

			}else{
				
			}
			
			
			// 1?????????
			try {
				TimeUnit.SECONDS.sleep(1);
				System.out.println("Test");
				//return new AsyncResult<Response>(typeresponse);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
		}
		
	}
	
	private Response PickupMessageForResponseQueue(StringBuilder destinationStatusCode, StringBuilder destinationResponseBody, 
	StringBuilder callbackUri, StringBuilder destinationresult, StringBuilder transactionid, StringBuilder asyncid, 
	StringBuilder executiontime) {
	    Responses response      = new Responses();
	    String receiveMessage = new String();
	    
	    destinationStatusCode.delete(0, destinationStatusCode.length());
    	destinationResponseBody.delete(0, destinationResponseBody.length());
    	callbackUri.delete(0, callbackUri.length());
		destinationresult.delete(0, destinationresult.length());
		transactionid.delete(0, transactionid.length());
		asyncid.delete(0, asyncid.length());
		executiontime.delete(0, executiontime.length());
    	
	    // ???????????????????????? Properties ???????????????????????????????????????
	    Properties properties = new Properties();
	    // ????????? Kafka ?????????
	    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "asynckafka1:9092");
	    // Consumer ???????????????????????? group id
	    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "java-consumer-group");
	    // ??????????????????????????? offset ?????????????????????????????????Kafka ????????????????????????????????????????????????????????????
	    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	    // ???????????????????????????????????????????????? offset ????????????????????????
	    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
	    // ??????1???????????????????????????????????????
	    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
	 
	    // Consumer ???????????????
	    KafkaConsumer<String, String> consumer =
	        new KafkaConsumer<String, String>(properties, new StringDeserializer(), new StringDeserializer());
	 
	    // Consumer ?????????????????????????????????
	    consumer.subscribe(Arrays.asList("Async4BackendRest_CallbackAPI_Topic"));
	 
	    try {
	      while (true) {
	        // ??????????????????????????????
	        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(60l));
	        Date date = new Date();
    		SimpleDateFormat dateformat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    		String strDate = dateformat.format(date);
	        
	        // ??????????????????????????????????????????????????????
			if(0 ==  records.count()) {
				consumer.close();
				return Response.status(Response.Status.CREATED.getStatusCode()).entity(response).build();
			}
	        
	        // ?????????????????????????????????????????????
	        for (ConsumerRecord<String, String> record : records) {
	          receiveMessage = record.value();
			  System.out.println(String.format("*record* %s:%s", record.offset(), record.value()));
	        }
			destinationResponseBody.toString().replaceAll("\"", "\\\\\"");
	        // ????????????????????????????????????????????? offset ??????????????? poll() ???????????????(???????????????)????????????
			try{
			consumer.commitSync();
			JSONObject jsonObject = new JSONObject(receiveMessage);
			
			destinationStatusCode.append(jsonObject.getString("destinationStatusCode"));

			destinationResponseBody.append(jsonObject.getString("destinationResponseBody"));
			
			callbackUri.append(jsonObject.getString("callbackUri"));

			destinationresult.append(jsonObject.getString("destinationresult"));

	        transactionid.append(jsonObject.getString("transactionid"));

			asyncid.append(jsonObject.getString("asyncid"));

			executiontime.append(jsonObject.getString("executiontimes"));
			}catch(CommitFailedException e){
				e.printStackTrace();
			}
			return Response.status(Response.Status.CREATED.getStatusCode()).entity(response).build();
	      }
	    } catch(KafkaException e) {
			e.printStackTrace();
			System.out.println(e);
	    	return GeneralErrorRes(ErrorResponseEnum.E500);
	    } finally {
	      consumer.close();
	    }
	  }

	private Response ExecuteCallBackAPI(StringBuilder callbackUri,
			StringBuilder destinationStatusCode, StringBuilder destinationResponseBody, StringBuilder transactionid, StringBuilder asyncid, 
			StringBuilder executiontime,  StringBuilder destinationresult) {

			Responses response = new Responses();
			String requestBody = new String();
			String replacedStr = new String();
			String callbackResponseBody = new String();
			String DestinationStatusCode = new String();
			String DestinationResponseBody = new String();
			String CallbackUri = new String();
			String Destinationresult = new String();
			String Transactionid = new String();
			String Asyncid = new String();
			String Executiontime = new String();
			String sendMessage = new String();
			int callbackstatus;
			int Executiontime1;

		try {
	
			replacedStr = destinationResponseBody.toString().replaceAll("\"", "\\\\\"");
			requestBody = "{"
					+ "\"statuscode\":\"" 
					+ destinationStatusCode.toString() 
					+ "\",\"executiontime\":\"" 
					+ executiontime.toString() 
					+ "\",\"destinationResponseCode\":\""
					+ replacedStr + "\"}\"";	


			URL url = new URL(callbackUri.toString());
			
			HttpURLConnection urlConnection = null;
			
			urlConnection = (HttpURLConnection) url.openConnection();
			
			urlConnection.setRequestMethod("POST");
			
			urlConnection.setUseCaches(false);
			
			urlConnection.setDoOutput(true);
			
			urlConnection.setRequestProperty("Content-Type", "application/json; charset=utf-8");

			urlConnection.setRequestProperty("transactionid", transactionid.toString());

			urlConnection.setRequestProperty("async-id", asyncid.toString());
			
			OutputStreamWriter out = new OutputStreamWriter(
					new BufferedOutputStream(urlConnection.getOutputStream()));
			
			out.write(requestBody);
			
			out.close();
			
			// ??????????????????????????????
			callbackstatus = urlConnection.getResponseCode();
			System.out.println("CallbackStatus" + urlConnection.getResponseCode());
			Executiontime = executiontime.toString();
			Executiontime1 = Integer.parseInt(Executiontime);
			System.out.println(callbackstatus);
			if(callbackstatus == 204){
				System.out.println("Test001");
				if(Executiontime1 < 3){
					DestinationStatusCode = destinationStatusCode.toString();
					System.out.println(DestinationStatusCode);
					DestinationResponseBody = destinationResponseBody.toString();
					System.out.println(DestinationResponseBody);
					CallbackUri = callbackUri.toString();
					System.out.println(CallbackUri);
					Destinationresult = destinationresult.toString();
					System.out.println(Destinationresult);
					Transactionid = transactionid.toString();
					System.out.println(Transactionid);
					Asyncid = asyncid.toString();
					System.out.println(Asyncid);
					Executiontime1 += 1;
					Executiontime = String.valueOf(Executiontime1);
					System.out.println(Executiontime1);
					
					sendMessage = "http_code::" 
					+ DestinationStatusCode + "http_code::" 
					+ DestinationResponseBody;
					
					sendMessage = "{\"destinationStatusCode\":\"" 
					+ destinationStatusCode 
					+ "\"," 
					+ "\"destinationResponseBody\":\"" 
					+ replacedStr 
					+ "\"," 
					+ "\"callbackUri\":\"" 
					+ callbackUri 
					+ "\"," 
					+ "\"destinationresult\":\"" 
					+ destinationresult 
					+ "\","
					+ "\"transactionid\":\"" 
					+ transactionid 
					+ "\","
					+ "\"asyncid\":\"" 
					+ asyncid 
					+ "\","
					+ "\"executiontimes\":\"" 
					+ Executiontime + "\"}";

					Properties properties = new Properties();
		
					properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "asynckafka1:9092");
	
					KafkaProducer<String, String> producer = new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer());

					try {
						producer.send(new ProducerRecord<String, String>("Async4BackendRest_CallbackAPI_Topic", sendMessage));
						System.out.println("Test45" + sendMessage);
					} catch(KafkaException e) {
						return GeneralErrorRes(ErrorResponseEnum.E500);
					} finally {
						producer.close();
					}
				}else{
					System.out.println("???????????????");
				}
				
			}else{
			
				BufferedReader br = new BufferedReader(new InputStreamReader(urlConnection.getInputStream(), "UTF-8"));
				System.out.println("br: " + br);
				String line;
				System.out.println("callbackstatus: " + callbackResponseBody);
				while((line = br.readLine()) != null) {
					callbackResponseBody = line;
				}
				
				br.close();
			}
			
		} catch (Exception e) {
			return GeneralErrorRes(ErrorResponseEnum.E500);
		}
		return Response.status(Response.Status.CREATED.getStatusCode()).entity(response).build();
	}
	
}
