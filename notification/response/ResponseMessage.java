package com.open.jp.notification.response;

import java.util.List;

import javax.enterprise.context.RequestScoped;

@RequestScoped
public class ResponseMessage {
	
	private String time;
	
	private List<ResponseMessageBodyIDList> idList;
	private ResponseMessageBodyMessage message;
	
	public void setIdList(List<ResponseMessageBodyIDList> idList) {
			this.idList = idList;
	}
	
	public List<ResponseMessageBodyIDList> getIdList(){
		return idList;
	}
	
	public void setMessage(ResponseMessageBodyMessage message) {
		this.message = message;
	}
	
	public ResponseMessageBodyMessage getMessage(){
		return message;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}
}

