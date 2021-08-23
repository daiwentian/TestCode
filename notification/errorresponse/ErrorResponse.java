package com.open.jp.notification.errorresponse;

import javax.enterprise.context.RequestScoped;

@RequestScoped
public class ErrorResponse {
	
	private String code;
	
	private String time;
	
	private String message;

	
	public ErrorResponse() {

		this.setCode(new String());
		this.setTime(new String());
		this.setMessage(new String());
		
	}


	public String getCode() {
		return code;
	}


	public void setCode(String code) {
		this.code = code;
	}


	public String getTime() {
		return time;
	}


	public void setTime(String time) {
		this.time = time;
	}


	public String getMessage() {
		return message;
	}


	public void setMessage(String message) {
		this.message = message;
	}

}

