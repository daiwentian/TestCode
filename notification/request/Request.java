package com.open.jp.notification.request;


import javax.enterprise.context.RequestScoped;
import javax.ws.rs.HeaderParam;


@RequestScoped
public class Request {
	
    /**
     * Header,Path,Query..etc
     */
    @HeaderParam("TRX-ID")
    private String trxId;
    

    
    public void setTrxId(String trxId) {
    	this.trxId = trxId;
    }
    
    public String getTrxId() {
    	return this.trxId;
    }

}

