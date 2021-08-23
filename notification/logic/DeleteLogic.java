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
import com.open.jp.notification.response.Responses;
import com.open.jp.notification.util.StringUtil;

@RequestScoped
public class DeleteLogic {
  
  @Inject
  private DataSourceHolder ds;
  
  private StringUtil su = new StringUtil();
  
  public DeleteLogic() {
      
  }
  
  public  Response GeneratedErrorRes(ErrorResponseEnum errorresponseenum) {
    
    ErrorResponse erroresponse = new ErrorResponse();
    
    erroresponse.setTime(LocalDateTime.now().toString()); 
    erroresponse.setCode(errorresponseenum.getErrorStatusCodeMessage()); 
    erroresponse.setMessage(errorresponseenum.getErrorMessage()); 
    return Response.status(errorresponseenum.getErrorStatus()).entity(erroresponse).build();
  }
  
  public Response service(StringBuilder id) {
    
    Responses response = new Responses();
    
    Response IdListCheckResponse = IdNullCheck(id);
    if(IdListCheckResponse.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
      return IdListCheckResponse;
    }
    
    IdListCheckResponse = GeneratedbConnectionGetseqId(id);
    if(IdListCheckResponse.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
      return IdListCheckResponse;
    }
    
    IdListCheckResponse = GeneratebConnectionDelete(id);
    if(IdListCheckResponse.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
    return IdListCheckResponse;
    }
    response.setTime(LocalDateTime.now().toString());
    return Response.status(Response.Status.NO_CONTENT.getStatusCode()).entity(response).build();
      
    }
  
  private Response IdNullCheck(StringBuilder id) {
    
    Responses response = new Responses();
    
    if( su.isNullorEmpty(id.toString()) ) {
      return  GeneratedErrorRes(ErrorResponseEnum.E400);
    }
    return Response.status(Response.Status.NO_CONTENT.getStatusCode()).entity(response).build();
  }

  
  private Response GeneratedbConnectionGetseqId(StringBuilder id){
    
    String user_seq_id = new String();
    Responses response = new Responses();
    String strSQLState = new String();
    
    try(Connection connection = ds.newConnection();
          PreparedStatement preparedStatement = connection.prepareStatement("SELECT USER_SEQ_ID FROM T_TOKEN"
              + " WHERE USER_ID = ? ORDER BY UPDATE_DATE DESC LIMIT 1 FOR UPDATE NOWAIT");){
                      preparedStatement.setString(1, id.toString());
          try(ResultSet resultset = preparedStatement.executeQuery();){
              if(resultset.next()) {
                      user_seq_id = resultset.getString("USER_SEQ_ID");
              } else {
                      return  GeneratedErrorRes(ErrorResponseEnum.E404);
              }
        
          }catch(SQLException e) {
                strSQLState = e.getSQLState();
                if("55P03".equals(strSQLState) ) {
                  return  GeneratedErrorRes(ErrorResponseEnum.E409DEL);
                }else {
                  e.printStackTrace();
                  return  GeneratedErrorRes(ErrorResponseEnum.E500);
                }
          }
    } catch (SQLException e1) {
         e1.printStackTrace();
         return  GeneratedErrorRes(ErrorResponseEnum.E500);
    }
         return Response.status(Response.Status.NO_CONTENT.getStatusCode()).entity(response).build();
 }
  
  private Response GeneratebConnectionDelete(StringBuilder id){
    
    Responses response = new Responses();
    
    int updateCount = 0;
    
    try(Connection con = ds.newConnection();
        PreparedStatement ps = con.prepareStatement("DELETE FROM T_TOKEN"
            + " WHERE USER_ID = ?");){
      
      ps.setString(1, id.toString());
      
      updateCount = ps.executeUpdate();
      
      if(0 > updateCount) {
        return  GeneratedErrorRes(ErrorResponseEnum.E500);
      }
    }
      catch(SQLException e) {
        e.printStackTrace();
        return GeneratedErrorRes(ErrorResponseEnum.E500);
     }
    return Response.status(Response.Status.NO_CONTENT.getStatusCode()).entity(response).build();
  }
}


