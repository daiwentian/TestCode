package com.open.jp.async.service;

import java.util.Map;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.BeanParam;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.open.jp.async.logic.PostCheckLogic;
import com.open.jp.async.logic.PostLogic;
import com.open.jp.async.request.Request;
import com.open.jp.async.request.RequestBody;

@RequestScoped
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Path("")
public class ServiceResource {
	
	@Inject
	PostLogic postLogic;
	
	@Inject
	PostCheckLogic postcheckLogic;
    
    @POST
    public Response postBean(@BeanParam Request request, Map<String,Object> body) {

    	return postLogic.service(request, body);
    }
    
    @POST
    @Path("/asynccheck")
    public Response postBeanCheck(@BeanParam Request request, RequestBody body) {

    	return postcheckLogic.service(request, body);
    }

}

