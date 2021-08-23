package com.open.jp.async.response;

import javax.enterprise.context.RequestScoped;

@RequestScoped
public class Responses {

	private String message;
	
	private String time;
	
	private String asyncid;
	
	private String transactionid;

	
	public Responses() {

		this.message = new String();
		this.time  = new String();
		this.asyncid  = new String();
		this.transactionid = new String();
		
	}
	
	public void setMessage(String message) {
		
		this.message = message;
		
	}

	public String getMessage() {

		return this.message;

	}

	public void setTime(String time) {
		
		this.time = time;
		
	}

	public String getTime() {

		return this.time;

	}
	
	public void setAsyncid(String asyncid) {
		
		this.asyncid = asyncid;
		
	}

	public String getAsyncid() {
		
		return this.asyncid;
		
	}
	
	public void setTransactionid(String transactionid) {
		
		this.transactionid = transactionid;
		
	}

	public String getTransactionid() {

		return this.transactionid;

	}
}

