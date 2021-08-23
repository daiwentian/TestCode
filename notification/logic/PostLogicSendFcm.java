package com.open.jp.notification.logic;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public class PostLogicSendFcm {
	
	public PostLogicSendFcm() {
		
	}

	public int sendfcm(String BODY) {
		
		String KEY = "key=AAAA17G2kug:APA91bHRm-VO4Lpip4iTWyrFtsQUQj4vaCR2b2dt1HcqVoZPvFQH6bRG1AU0l1aP6Rk1l_E0bNIHR_x-nGpnR4fcatamjweLJjD3bntG6hLu86tnkYPO5uuSvaco6thnFw06D4DwwrSQ";
		
		Client client = ClientBuilder.newClient();
		WebTarget target = client.target("https://fcm.googleapis.com/fcm/send"); 
		Builder req = target.request(MediaType.TEXT_PLAIN_TYPE); 
		req.header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
				.header(HttpHeaders.AUTHORIZATION, KEY);
		Response fcmresponse = req.post(Entity.<String> entity(BODY, MediaType.APPLICATION_JSON));
		
		return fcmresponse.getStatus();
	}
}

