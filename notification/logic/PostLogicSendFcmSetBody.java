package com.open.jp.notification.logic;

import com.open.jp.notification.request.message.RequestMessageBody;

public class PostLogicSendFcmSetBody {

	public PostLogicSendFcmSetBody() {
	}
		
	public String SetBody(RequestMessageBody body, String token) {
		
		String BODY = "{\"to\": \""
	    	+ token + "\",\"notification\":{\"title\":\""
	    	+ body.getMessage().getTitle()
	    	+ "\",\"body\":\""
	    	+ body.getMessage().getText()
	    	+ "\"},\"priority\":10,\"mutable_content\":true}";
		
		return BODY;
	}
}


