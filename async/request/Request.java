package com.open.jp.async.request;


import javax.enterprise.context.RequestScoped;
import javax.ws.rs.HeaderParam;


@RequestScoped
public class Request {

    @HeaderParam("Transaction-Id")
    private String trxId;
    

    
    public void setTrxId(String trxId) {
    	this.trxId = trxId;
    }
    
    public String getTrxId() {
    	return this.trxId;
    }

}

