package com.open.jp.notification.request;


import javax.enterprise.context.RequestScoped;
import javax.validation.constraints.NotBlank;

@RequestScoped
public class RequestBody {
	
	
	@NotBlank
	private String id;
	
	public void setId(String id) {
		
		this.id = id;
		
	}
	public String getId() {
		return this.id;
	}
	
	@NotBlank
	private String token;
	
	public void setToken(String token) {
		
		this.token = token;
		
	}
	public String getToken() {
		return this.token;
	}
	
}

