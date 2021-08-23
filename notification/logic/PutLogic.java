package com.open.jp.notification.logic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;

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
public class PutLogic {
	
	@Inject
	private DataSourceHolder ds;
	
	private StringUtil stringutil = new StringUtil();
	
	public PutLogic() {
		
	}
	
	public  Response GeneratedErrorRes(ErrorResponseEnum errorresponseenum) {
	    
	    ErrorResponse erroresponse = new ErrorResponse();
	    
	    erroresponse.setTime(LocalDateTime.now().toString()); 
	    erroresponse.setCode(errorresponseenum.getErrorStatusCodeMessage()); 
	    erroresponse.setMessage(errorresponseenum.getErrorMessage()); 
	    return Response.status(errorresponseenum.getErrorStatus()).entity(erroresponse).build();
	}
	
	public Response service(Request request, RequestBody body, String id) {
		
		Responses response = new Responses();
		
		StringBuilder userSeqID = new StringBuilder();
		
		Response typeresponse = bodyTypeCheck(body);
		
		if(typeresponse.getStatus() != Response.Status.OK.getStatusCode()) {
		      return typeresponse;
		}
		
		typeresponse = idTypeCheck(body, id);
		if(typeresponse.getStatus() != Response.Status.OK.getStatusCode()) {
		      return typeresponse;
		}
		
		typeresponse = GenerateddbConnectionGetseqId(id, userSeqID);
	    
	    if(typeresponse.getStatus() != Response.Status.OK.getStatusCode()) {
	      return typeresponse;
	    }
	    
	    typeresponse = GenerateddbConnectionUpdate(userSeqID, body);
	    
	    if(typeresponse.getStatus() != Response.Status.OK.getStatusCode()) {
		      return typeresponse;
		    }

    	response.setTime(LocalDateTime.now().toString());
    	response.setToken(body.getToken());
    	response.setId(body.getId());
    	
    	return Response.status(Response.Status.OK.getStatusCode()).entity(response).build();
		
	}
	
	private Response bodyTypeCheck(RequestBody body) {
		
		Responses response = new Responses();
		
	    if (stringutil.isNullorEmpty(body.getId()) || stringutil.isNullorEmpty(body.getToken())) {
	    
	    	return  GeneratedErrorRes(ErrorResponseEnum.E400);
	    }
	  
	    return Response.status(Response.Status.OK.getStatusCode()).entity(response).build();
	}
	
	private Response idTypeCheck(RequestBody body, String id) {
		
		Responses response = new Responses();
		
	    if (stringutil.isNullorEmpty(id) || !body.getId().equals(id)) {
	    
	    	return  GeneratedErrorRes(ErrorResponseEnum.E400);
	    }
	  
	    return Response.status(Response.Status.OK.getStatusCode()).entity(response).build();
	}
	
	private Response GenerateddbConnectionGetseqId(String id, StringBuilder userSeqID) {
		String strUserSeqId = new String();
		Responses response = new Responses();
	    String strSQLState = new String();
	    
	    try(Connection connection = ds.newConnection();
	          PreparedStatement preparedStatement = connection.prepareStatement("SELECT USER_SEQ_ID FROM T_TOKEN"
	              + " WHERE USER_ID = ? ORDER BY UPDATE_DATE DESC LIMIT 1 FOR UPDATE NOWAIT");){
	                      preparedStatement.setString(1, id.toString());
	          try(ResultSet resultset = preparedStatement.executeQuery();){
	              if(resultset.next()) {
	            	  strUserSeqId = resultset.getString("USER_SEQ_ID");
	            	  userSeqID.append(strUserSeqId);
	              } else {
	                      return  GeneratedErrorRes(ErrorResponseEnum.E404);
	              }
	        
	          }catch(SQLException e) {
	                strSQLState = e.getSQLState();
	                if("55P03".equals(strSQLState) ) {
	                  return  GeneratedErrorRes(ErrorResponseEnum.E409PUT);
	                }else {
	                  e.printStackTrace();
	                  return  GeneratedErrorRes(ErrorResponseEnum.E500);
	                }
	          }
	    } catch (SQLException e1) {
	         e1.printStackTrace();
	         return  GeneratedErrorRes(ErrorResponseEnum.E500);
	    }
	         return Response.status(Response.Status.OK.getStatusCode()).entity(response).build();
	}
	
	private Response GenerateddbConnectionUpdate(StringBuilder userSeqID, RequestBody body) {
		
		Responses response = new Responses();
		int updateCount = 0;
		
		try(Connection con = ds.newConnection();
    		PreparedStatement ps = con.prepareStatement("UPDATE T_TOKEN"
    				+ " SET DEVICE_TOKEN = ?, UPDATE_DATE = current_timestamp"
    				+ " WHERE USER_SEQ_ID = ?");){

    		ps.setString(1, body.getToken());
    		ps.setString(2, userSeqID.toString());

    		updateCount = ps.executeUpdate();

    		if(updateCount != 1) {
    			return  GeneratedErrorRes(ErrorResponseEnum.E500);
            }
    		
    	}catch(SQLException e) {
    		return  GeneratedErrorRes(ErrorResponseEnum.E500);
    	}
		
		return Response.status(Response.Status.OK.getStatusCode()).entity(response).build();
	}
}

