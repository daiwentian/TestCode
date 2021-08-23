package com.open.jp.notification.logic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Date;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.core.Response;

import com.open.jp.notification.common.DataSourceHolder;
import com.open.jp.notification.errorresponse.ErrorResponse;
import com.open.jp.notification.request.Request;
import com.open.jp.notification.request.RequestBody;
import com.open.jp.notification.response.Responses;
import com.open.jp.notification.util.StringUtil;

@RequestScoped
public class PostLogic {
	
	@Inject
	private DataSourceHolder datasourceholder;
	
	private StringUtil stringutil = new StringUtil();
	
	public PostLogic() {
		
	}
	
	public  Response GeneralErrorRes(ErrorResponseEnum errorresponseenum) {
	    ErrorResponse erroresponse = new ErrorResponse();
	    erroresponse.setTime(LocalDateTime.now().toString()); 
	    erroresponse.setCode(errorresponseenum.getErrorStatusCodeMessage()); 
	    erroresponse.setMessage(errorresponseenum.getErrorMessage()); 
	    return Response.status(errorresponseenum.getErrorStatus()).entity(erroresponse).build();
	}

	public Response service(Request request, RequestBody body) {
		
		Responses response = new Responses();
		StringBuilder seqID = new StringBuilder();
		
		Response typeresponse = bodyTypeCheck(body);
		
	    if(typeresponse.getStatus() != Response.Status.CREATED.getStatusCode()) {
	      return typeresponse;
	    }
	    
	    typeresponse = GenerateddbConnectionGetseqId(seqID);
	    
	    if(typeresponse.getStatus() != Response.Status.CREATED.getStatusCode()) {
	      return typeresponse;
	    }
	    
	    typeresponse = GeneratedbConnectionInsert(body,seqID);
	   
	    if(typeresponse.getStatus() != Response.Status.CREATED.getStatusCode()) {
	      return typeresponse;
	    }
	    
    	response.setTime(LocalDateTime.now().toString());
    	response.setToken(body.getToken());
    	response.setId(body.getId());
    	
    	return Response.status(Response.Status.CREATED.getStatusCode()).entity(response).build();
		
	}
	
	private Response bodyTypeCheck(RequestBody body) {
		
		Responses response = new Responses();
		
	    if (stringutil.isNullorEmpty(body.getId()) || stringutil.isNullorEmpty(body.getToken())) {
	    
	    	return  GeneralErrorRes(ErrorResponseEnum.E400);
	    }
	  
	    return Response.status(Response.Status.CREATED.getStatusCode()).entity(response).build();
	}
	
	private Response GenerateddbConnectionGetseqId(StringBuilder seqID) {
		
		Responses response      = new Responses();
		String su = new String();
		

	    try(Connection connection = datasourceholder.newConnection();
	            PreparedStatement preparedStatement = connection.prepareStatement("SELECT NEXTVAL('UserSeqID')");
	              ResultSet resultset = preparedStatement.executeQuery();){
	    		boolean br = false;
	    		br = resultset.next();
	    		
	    		Date date = new Date();
	    		SimpleDateFormat dateformat = new SimpleDateFormat("yyyyMMdd");
	    		String strDate = dateformat.format(date);
	    		su   = strDate + String.format("%07d", resultset.getInt("nextval"));
	    		seqID.append(su);
	    }catch(SQLException e) {
	    	e.printStackTrace();
	   
	    	return GeneralErrorRes(ErrorResponseEnum.E500);
	    }
	   
	    return Response.status(Response.Status.CREATED.getStatusCode()).entity(response).build();
	    
	}

	private Response GeneratedbConnectionInsert(RequestBody body, StringBuilder seqID) {
	    
	    Responses response      = new Responses();
	    String strSQLState      = new String();
	    
	    int insertCount = 0;
	   
	    try(Connection connection = datasourceholder.newConnection();
	            PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO T_TOKEN("
	            + "USER_SEQ_ID,"
	            + "USER_ID,"
	            + "DEVICE_TOKEN,"
	            + "CREATE_DATE,"
	            + "UPDATE_DATE"
	            + ") VALUES("
	            + "?,"
	            + "?,"
	            + "?,"
	            + "current_timestamp,"
	            + "current_timestamp)");){
	        
	        preparedStatement.setString(1, seqID.toString());
	        preparedStatement.setString(2, body.getId());
	        preparedStatement.setString(3, body.getToken());
	        insertCount = preparedStatement.executeUpdate();
	          
	          if(insertCount != 1) {
	     
	            return GeneralErrorRes(ErrorResponseEnum.E500);
	          }
	        }catch(SQLException e) {
	          
	          strSQLState = e.getSQLState();
	          e.printStackTrace();
	         
	          if("23505".contentEquals(strSQLState)) {
	         
	            return GeneralErrorRes(ErrorResponseEnum.E409POST) ;
	            }else {
	           
	              return GeneralErrorRes(ErrorResponseEnum.E500);
	            }
	          }
	        
	    response.setTime(LocalDateTime.now().toString());
	    response.setToken(body.getToken());
	    response.setId(body.getId());

	    return Response.status(Response.Status.CREATED.getStatusCode()).entity(response).build();
	  }
	
}

