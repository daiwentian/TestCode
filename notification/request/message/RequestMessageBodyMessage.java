package com.open.jp.notification.request.message;

import javax.enterprise.context.RequestScoped;
import javax.validation.constraints.NotBlank;

@RequestScoped
public class RequestMessageBodyMessage {
	
	@NotBlank
	private String title;
	private String text;
	
	public void setTitle(String title) {
		
		this.title = title;
		
	}
	public String getTitle() {
		return this.title;
	}
	
	public void setText(String text) {
		
		this.text = text;
		
	}
	public String getText() {
		return this.text;
	}

}

