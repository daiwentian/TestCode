package com.open.jp.async.logic;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import javax.enterprise.context.RequestScoped;
import javax.ws.rs.core.Response;

import com.open.jp.async.errorresponse.ErrorResponse;
import com.open.jp.async.request.Request;
import com.open.jp.async.response.Responses;
import com.open.jp.async.util.StringUtil;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;


@RequestScoped
public class PostLogic {
	
	
	private StringUtil stringutil = new StringUtil();
	
	public PostLogic() {
		
	}
	
		/**
	 * Description<br>
	 * エラー時のレスポンスを生成するメソッド.<br>
	 * 
	 * ErrorResponseEnumで定義されている、ErrorStatusCodeMessage、ErrorMessage、ErrorStatusをErrorResponseクラスの
	 * Time、Code、Messageにセットし各エラーステータスコードによって呼び出す際に利用する.<br>
	 * 
	 * @version 2.0
	 * @param errorresponseenum "E400" "E409" "E500" ErrorResponseEnumで定義されたエラーコードを引数とする.
	 * @return エラーの場合は"ErrorResponseEnum"クラスで定義されている各エラーとそのメッセージを返す。
	 */
	public  Response GeneralErrorRes(ErrorResponseEnum errorresponseenum) {
	    ErrorResponse erroresponse = new ErrorResponse();
	    erroresponse.setTime(LocalDateTime.now().toString()); 
	    erroresponse.setCode(errorresponseenum.getErrorStatusCodeMessage()); 
	    erroresponse.setMessage(errorresponseenum.getErrorMessage()); 
	    return Response.status(errorresponseenum.getErrorStatus()).entity(erroresponse).build();
	}
	
	public Response service(Request request, Map<String,Object> body) {
		
		Responses response = new Responses();
		StringBuilder asyncID = new StringBuilder();
		StringBuilder inputStrings = new StringBuilder();
		
		final String NORMAL_END = "Successfull";
		
		Response typeresponse = headerTypeCheck(request);
		
	    if(typeresponse.getStatus() != Response.Status.CREATED.getStatusCode()) {
	      return typeresponse;
	    }

		typeresponse = bodyTypeCheck(body);
		
	    if(typeresponse.getStatus() != Response.Status.CREATED.getStatusCode()) {
	      return typeresponse;
	    }
	    
		typeresponse = GenerateAsyncId(asyncID);
		   
	    if(typeresponse.getStatus() != Response.Status.CREATED.getStatusCode()) {
	      return typeresponse;
	    }

	    typeresponse = GenerateInputStrings(request, body, asyncID, inputStrings);
		   
	    if(typeresponse.getStatus() != Response.Status.CREATED.getStatusCode()) {
	      return typeresponse;
	    }
		
		typeresponse = RegisteMessageForExecQueue(inputStrings);
		   
		if(typeresponse.getStatus() != Response.Status.CREATED.getStatusCode()) {
		  return typeresponse;
		}
		
    	response.setTime(LocalDateTime.now().toString());
    	response.setMessage(NORMAL_END);
    	response.setTransactionid(request.getTrxId().toString());
    	response.setAsyncid(asyncID.toString());
    	
    	return Response.status(Response.Status.CREATED.getStatusCode()).entity(response).build();
		
	}
	
	/**
	 * Description<br>
	 * リクエストされたTransaction-Idのパラメータをチェックするメソッド.<br>
	 * 
	 * パラメータのheaderに入っているTransaction-IdのnullチェックをStringUtilのisNullorEmptyメソッドにより型booleanで判定する.<br>
	 * nullの場合はtrueを返し、値が入っている場合はfalseで返す.<br>
	 * nullの場合、HTTPステータスコード400を返す.<br>
	 * パラメータ違反（null）ではない場合は、HTTPステータスコード201を返す.<br>
	 * 
	 *  @version 2.0
	 *  @see Request
	 *  @param request Request型の入力項目
	 *  @return Status.CREATED
	 */
	private Response headerTypeCheck(Request request) {
		
		Responses response = new Responses();
		
		// Transaction-Idのチェック
	    if (stringutil.isNullorEmpty(request.getTrxId().toString())) {

	    	return  GeneralErrorRes(ErrorResponseEnum.E400);
	    }
	  
	    return Response.status(Response.Status.CREATED.getStatusCode()).entity(response).build();
	}
	
	/**
	 * Description<br>
	 * リクエストされたdestinationuri,destinationhttpmethod,callbackuriのパラメータをチェックするメソッド.<br>
	 * 
	 * パラメータのbodyに入っているdestinationuri,destinationhttpmethod,callbackuriのnullチェックをStringUtilのisNullorEmptyメソッドにより型booleanで判定する.<br>
	 * nullの場合はtrueを返し、値が入っている場合はfalseで返す.<br>
	 * いずれか(destinationuri,destinationhttpmethod,callbackuri)がnullの場合、HTTPステータスコード400を返す.<br>
	 * すべてパラメータ違反（null）ではない場合は、HTTPステータスコード201を返す.<br>
	 * 
	 *  @version 2.0
	 *  @see Map<String,Object>
	 *  @param body Map<String,Object>型の入力項目
	 *  @return Status.CREATED
	 */
	private Response bodyTypeCheck(Map<String,Object> body) {
		
		Responses response = new Responses();
		
		// destinationuriのチェック
	    if (stringutil.isNullorEmpty(body.get("destinationuri").toString())) {
	    
	    	return  GeneralErrorRes(ErrorResponseEnum.E400);
	    }

		// destinationhttpmethodのチェック
	    if (stringutil.isNullorEmpty(body.get("destinationhttpmethod").toString())) {
	    
	    	return  GeneralErrorRes(ErrorResponseEnum.E400);
	    }
		
		// callbackuriのチェック
	    if (stringutil.isNullorEmpty(body.get("callbackuri").toString())) {
	    
	    	return  GeneralErrorRes(ErrorResponseEnum.E400);
	    }

		// destinationrequestheaderがある場合
		if(body.containsKey("destinationrequestheader")) {
			// destinationrequestheaderのチェック
			if (stringutil.isNullorEmpty(body.get("destinationrequestheader").toString())) {
	    
				return  GeneralErrorRes(ErrorResponseEnum.E400);
			}
		}

		// destinationrequestbodyがある場合
		if(body.containsKey("destinationrequestbody")) {
			// destinationrequestheaderのチェック
			if (stringutil.isNullorEmpty(body.get("destinationrequestbody").toString())) {
	    
				return  GeneralErrorRes(ErrorResponseEnum.E400);
			}
		}
	  
	    return Response.status(Response.Status.CREATED.getStatusCode()).entity(response).build();
	}
	
	/**
	 * Description<br>
	 * シーケンスIDの生成を行うメソッド.<br>
	 * 
	 * リクエストを受けた後、パラメータ違反がない場合に実行される。.<br>
	 * 
     *  @version 2.0
     *  @see java.lang.StringBuilder
     *  @param asyncID StringBuilderクラス (メソッドで生成した文字列を格納する.）
     *  @return  Status.CREATED
     */
	private Response GenerateAsyncId(StringBuilder asyncID) {
		
		Responses response      = new Responses();

		// asyncIDの初期化
	    asyncID.delete(0, asyncID.length());

		// UUIDを生成してasyncIDに登録
		asyncID.append(UUID.randomUUID().toString());
	   
	    return Response.status(Response.Status.CREATED.getStatusCode()).entity(response).build();
	    
	}
	
	/**
	 * Description<br>
	 * RequesBodyの内容をInputJsonクラスに格納するメソッド.<br>
	 * 
	 * RequestBodyの内容をInputJsonクラスに格納する。
     * 
     *  @version 2.0
     *  @see java.lang.StringBuilder
     *  @param body インプット項目のJSONをbodyに格納し引数にセット
     *  @param inputjson InputJsonクラス (bodyの内容を格納する.）
     *  @return Status.CREATED
     */
	private Response GenerateInputStrings(Request request, Map<String,Object> body, StringBuilder asyncID, StringBuilder inputstrings) {
	    
	    Responses response = new Responses();
		String replacebody = new String();
	    
		// inputstringsの初期化
	    inputstrings.delete(0, inputstrings.length());

		inputstrings.append("{");
		inputstrings.append("\"transactionid\"");
		inputstrings.append(":");
		inputstrings.append("\"");
		inputstrings.append(request.getTrxId());
		inputstrings.append("\"");
		inputstrings.append(",");
		inputstrings.append("\"destinationuri\"");
		inputstrings.append(":");
		inputstrings.append("\"");
		inputstrings.append(body.get("destinationuri").toString());
		inputstrings.append("\"");
		inputstrings.append(",");
		inputstrings.append("\"destinationhttpmethod\"");
		inputstrings.append(":");
		inputstrings.append("\"");
		inputstrings.append(body.get("destinationhttpmethod").toString());
		inputstrings.append("\"");

		// destinationrequestheaderがある場合
		if(body.containsKey("destinationrequestheader")) {
			inputstrings.append(",");
			inputstrings.append("\"destinationrequestheader\"");
			inputstrings.append(":");
			inputstrings.append("\"");
			inputstrings.append(body.get("destinationrequestheader").toString());
			inputstrings.append("\"");
		}

		// destinationrequestbodyがある場合
		if(body.containsKey("destinationrequestbody")) {
			// URLEncodeする
			try {
				replacebody = URLEncoder.encode(body.get("destinationrequestbody").toString(), "utf-8");
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return  GeneralErrorRes(ErrorResponseEnum.E400);
			}
			inputstrings.append(",");
			inputstrings.append("\"destinationrequestbody\"");
			inputstrings.append(":");
			inputstrings.append("\"");
			inputstrings.append(replacebody);
			inputstrings.append("\"");
		}

		inputstrings.append(",");
		inputstrings.append("\"callbackuri\"");
		inputstrings.append(":");
		inputstrings.append("\"");
		inputstrings.append(body.get("callbackuri").toString());
		inputstrings.append("\"");
		inputstrings.append(",");
		inputstrings.append("\"asyncid\"");
		inputstrings.append(":");
		inputstrings.append("\"");
		inputstrings.append(asyncID.toString());
		inputstrings.append("\"");
		inputstrings.append(",");
		inputstrings.append("\"executiontimes\"");
		inputstrings.append(":");
		inputstrings.append("\"");
		inputstrings.append("1");
		inputstrings.append("\"");
		inputstrings.append("}");

		System.out.println("inputstrings = " + inputstrings);

	    return Response.status(Response.Status.CREATED.getStatusCode()).entity(response).build();
	  }
	
	/**
	 * Description<br>
	 * InputJsonクラスの内容を実行用キューに登録するメソッド.<br>
	 * 
	 * InputJsonの内容を文字列にして実行用キューに登録する。
     * 
     *  @version 2.0
     *  @see java.lang.StringBuilder
     *  @param body インプット項目のJSONをbodyに格納し引数にセット
     *  @param inputjson InputJsonクラス (bodyの内容を格納する.）
     *  @return Status.CREATED
     */
	private Response RegisteMessageForExecQueue(StringBuilder inputStrings) {
	    
	    Responses response      = new Responses();
	    String sendMessage = new String();
	    
	    sendMessage = inputStrings.toString();
	    
	    // 接続時の設定値を Properties インスタンスとして構築する
	    Properties properties = new Properties();
	    // 接続先 Kafka ノード
	    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "asynckafka1:9092");
	 
	    // Producer を構築する
	    KafkaProducer<String, String> producer = new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer());
	 
	    try {
	      // トピックを指定してメッセージを送信する
		  System.out.println("Send Message For Async4BackendRest_DestinationAPI_Topic = " + sendMessage);
	    	
	      producer.send(new ProducerRecord<String, String>("Async4BackendRest_DestinationAPI_Topic", sendMessage));

	    } catch(Exception e) {
	    	return GeneralErrorRes(ErrorResponseEnum.E500);
	    } finally {
			
	    	producer.close();
			
	    }
	    
	    return Response.status(Response.Status.CREATED.getStatusCode()).entity(response).build();
	  }
	
}

