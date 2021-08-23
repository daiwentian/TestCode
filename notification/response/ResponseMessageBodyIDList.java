package com.open.jp.notification.response;

import javax.enterprise.context.RequestScoped;
import javax.validation.constraints.NotBlank;

@RequestScoped
public class ResponseMessageBodyIDList {
	
	@NotBlank
	private String id;
	
	public void setId(String id) {
		
		this.id = id;
		
	}
	public String getId() {
		return this.id;
	}

}

