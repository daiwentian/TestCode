package com.open.jp.async.logic;

import java.time.LocalDateTime;
import java.util.concurrent.Future;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.core.Response;

import com.open.jp.async.request.Request;
import com.open.jp.async.request.RequestBody;
import com.open.jp.async.response.Responses;


@RequestScoped
public class PostCheckLogic {
	
	
	@Inject
	private Async4BackendRest_DestinationAPI_TopicCheckLogic async4backendrest_destinationapi_topicchecklogic;
	
	@Inject
	private PostTopic2CheckLogic posttopic2checklogic;
	
	public PostCheckLogic() {
		
	}
	
	public Response service(Request request, RequestBody body) {

		Responses response = new Responses();

		Future<Response> typeresponse = async4backendrest_destinationapi_topicchecklogic.ExecThreadAsync4BackendRest_DestinationAPI();

		//posttopic2checklogic.ExecThreadMethodForTopic2();


		response.setTime(LocalDateTime.now().toString());

		return Response.status(Response.Status.CREATED.getStatusCode()).entity(response).build();

	}
	
}

