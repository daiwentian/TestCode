package com.open.jp.notification.request.message;

import javax.enterprise.context.RequestScoped;

@RequestScoped
public class RequestMessageBodyIDList {
	
//	@NotBlank
	private String id;
	
	public void setId(String id) {
		
		this.id = id;
		
	}
	public String getId() {
		return this.id;
	}

}

