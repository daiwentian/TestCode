package com.open.jp.notification.request.message;

import java.util.List;

import javax.enterprise.context.RequestScoped;

@RequestScoped
public class RequestMessageBody {
	

	private List<RequestMessageBodyIDList> idList;
	private RequestMessageBodyMessage message;
	
	public void setIdList(List<RequestMessageBodyIDList> idList) {
		this.idList = idList;
	}
	
	public List<RequestMessageBodyIDList> getIdList(){
		return idList;
	}
	
	public void setMessage(RequestMessageBodyMessage message) {
		this.message = message;
	}
	
	public RequestMessageBodyMessage getMessage(){
		return message;
	}

}

