package com.open.jp.async.request;


import javax.enterprise.context.RequestScoped;
import javax.validation.constraints.NotBlank;

@RequestScoped
public class RequestBody {
	
	
	@NotBlank
	private String destinationuri;
	
	public void setDestinationuri(String destinationuri) {
		
		this.destinationuri = destinationuri;
		
	}
	public String getDestinationuri() {
		return this.destinationuri;
	}
	
	@NotBlank
	private String destinationhttpmethod;
	
	public void setDestinationhttpmethod(String destinationhttpmethod) {
		
		this.destinationhttpmethod = destinationhttpmethod;
		
	}
	public String getDestinationhttpmethod() {
		return this.destinationhttpmethod;
	}
	
	@NotBlank
	private String destinationrequestheader;
	
	public void setDestinationrequestheader(String destinationrequestheader) {
		
		this.destinationrequestheader = destinationrequestheader;
		
	}
	public String getDestinationrequestheader() {
		return this.destinationrequestheader;
	}
	
	@NotBlank
	private String destinationrequestbody;
	
	public void setDestinationrequestbody(String destinationrequestbody) {
				this.destinationrequestbody = destinationrequestbody;
		
	}
	public String getDestinationrequestBody() {
		return this.destinationrequestbody;
	}
	
	@NotBlank
	private String callbackuri;
	
	public void setCallbackuri(String callbackuri) {
		
		this.callbackuri = callbackuri;
		
	}
	public String getCallbackuri() {
		return this.callbackuri;
	}
	
	@NotBlank
	private String callbackurirequestheader;
	
	public void setCallbackurirequestheader(String callbackurirequestheader) {
		
		this.callbackurirequestheader = callbackurirequestheader;
		
	}
	public String getCallbackurirequestheader() {
		return this.callbackurirequestheader;
	}
}

