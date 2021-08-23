package com.open.jp.notification.response;

import javax.enterprise.context.RequestScoped;

@RequestScoped
public class Responses {

	private String token;
	
	private String time;
	
	private String id;

	
	public Responses() {

		this.token = new String();
		this.time  = new String();
		this.id  = new String();
		
	}
	
	public void setToken(String token) {
		
		this.token = token;
		
	}

	public String getToken() {

		return this.token;

	}

	public void setTime(String time) {
		
		this.time = time;
		
	}

	public String getTime() {

		return this.time;

	}
	
	public void setId(String id) {
		
		this.id = id;
		
	}

	public String getId() {
		
		return this.id;
		
	}


	
}

