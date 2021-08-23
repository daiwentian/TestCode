package com.open.jp.notification.service;

import java.util.Map;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.json.bind.JsonbBuilder;
import javax.ws.rs.BeanParam;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.open.jp.notification.logic.DeleteLogic;
import com.open.jp.notification.logic.PostLogic;
import com.open.jp.notification.logic.PutLogic;
import com.open.jp.notification.logic.message.PostMessageLogic;
import com.open.jp.notification.request.Request;
import com.open.jp.notification.request.RequestBody;
import com.open.jp.notification.request.message.RequestMessage;
import com.open.jp.notification.request.message.RequestMessageBody;

@RequestScoped
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Path("")
public class ServiceResource {
	
	@Inject
	PostLogic postTestLogic;
	
	@Inject
	PostMessageLogic postTestMessageLogic;
	
	@Inject
	PutLogic putTestLogic;
	
	@Inject
	DeleteLogic deleteTestLogic;

    
    @POST
    @Path("/map")
    public Response postMapTest(Map<String,Object> body) {
    	System.out.println(body.getOrDefault("token", ""));
        return Response.ok().build();
    }
    
    @POST
    @Path("/destinations")
    public Response postBeanTest(@BeanParam Request request, RequestBody body) {

    	return postTestLogic.service(request, body);
    }
    
    @POST
    @Path("/string")
    public Response postStringTest(String strBody) {
    	System.out.println(strBody);
    	RequestBody body = JsonbBuilder.create().fromJson(strBody, RequestBody.class);
    	System.out.println(body.getToken());
    	return Response.ok().build();
    }
    
    @POST
    @Path("/messages")
    public Response postMessagesTest(@BeanParam RequestMessage request, RequestMessageBody body) {

    	return postTestMessageLogic.service(request, body);
    }
    
    @PUT
    @Path("/destinations/{id}")
    public Response putBeanTest(@BeanParam Request request, RequestBody body, @PathParam("id") String id) {

    	return putTestLogic.service(request, body, id);
    }
    
    @DELETE
    @Path("/destinations/{id}")
    public Response deleteBeanTest(@PathParam("id") StringBuilder id) {

    	return deleteTestLogic.service(id);
    }

}

