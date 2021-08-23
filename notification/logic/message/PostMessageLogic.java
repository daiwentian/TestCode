package com.open.jp.notification.logic.message;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.core.Response;

import com.open.jp.notification.common.DataSourceHolder;
import com.open.jp.notification.errorresponse.ErrorResponse;
import com.open.jp.notification.logic.ErrorResponseEnum;
import com.open.jp.notification.logic.PostLogicSendFcm;
import com.open.jp.notification.logic.PostLogicSendFcmSetBody;
import com.open.jp.notification.request.message.RequestMessage;
import com.open.jp.notification.request.message.RequestMessageBody;
import com.open.jp.notification.response.ResponseMessage;
import com.open.jp.notification.response.ResponseMessageBodyIDList;
import com.open.jp.notification.response.ResponseMessageBodyMessage;
import com.open.jp.notification.util.StringUtil;


@RequestScoped
public class PostMessageLogic {
  
  @Inject
  private DataSourceHolder ds;
  
  private StringUtil stringutil = new StringUtil();
  
  public PostMessageLogic() {
      
  }
  
  public  Response GenerateErrorRes(ErrorResponseEnum errorresponseenum) {
      ErrorResponse erroresponse = new ErrorResponse();
      erroresponse.setTime(LocalDateTime.now().toString()); 
      erroresponse.setCode(errorresponseenum.getErrorStatusCodeMessage());
      erroresponse.setMessage(errorresponseenum.getErrorMessage());
      return Response.status(errorresponseenum.getErrorStatus()).entity(erroresponse).build();
  }
  
  public Response service(RequestMessage request, RequestMessageBody body) {
    
    StringBuilder seqMID = new StringBuilder();
    ResponseMessage response = new ResponseMessage();
    StringBuilder token = new StringBuilder();
    
    List<ResponseMessageBodyIDList> idList = new  ArrayList<>();
    Response IdListCheckResponse = IdListCheck(body);
    
    ResponseMessageBodyIDList id = new ResponseMessageBodyIDList();
    
    if(IdListCheckResponse.getStatus() != Response.Status.CREATED.getStatusCode()) {
      return IdListCheckResponse;
    }
    
    for(int loopcnt = 0; loopcnt < body.getIdList().size(); ++loopcnt) {
      
      IdListCheckResponse = EmptyidListCheck(loopcnt, body);
      if(IdListCheckResponse.getStatus() != Response.Status.CREATED.getStatusCode()) {
        return IdListCheckResponse;
      }
      
      IdListCheckResponse = GenerateDbConnectionGetDeviceTokenSelect(token, loopcnt, body);
      if(IdListCheckResponse.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
        continue;
      }else if(IdListCheckResponse.getStatus() != Response.Status.CREATED.getStatusCode()) {
        return IdListCheckResponse;
      }
      
      IdListCheckResponse = GenerateDbConnectiontgetMessageSeqIDSelect(seqMID);
      if(IdListCheckResponse.getStatus() != Response.Status.CREATED.getStatusCode()) {
        return IdListCheckResponse;
      }
      
      IdListCheckResponse = GenerateDbConnectioninsert(token, loopcnt, body, seqMID);
      if(IdListCheckResponse.getStatus() != Response.Status.CREATED.getStatusCode()) {
        return IdListCheckResponse;
      }
      
      IdListCheckResponse = GenerateDbConnectionupdate(token, loopcnt, seqMID, body);
      if(IdListCheckResponse.getStatus() != Response.Status.CREATED.getStatusCode()) {
        return IdListCheckResponse;
    }
      
      IdListCheckResponse = GenerateFCMResponse(token, loopcnt, seqMID, body);
      if(IdListCheckResponse.getStatus() != Response.Status.CREATED.getStatusCode()) {
        return IdListCheckResponse;
    }
      id.setId(body.getIdList().get(loopcnt).getId());
      idList.add(id);
  }
    
    response.setTime(LocalDateTime.now().toString());
    response.setIdList(idList);
    ResponseMessageBodyMessage message = new ResponseMessageBodyMessage();
    message.setTitle(body.getMessage().getTitle());
    message.setText(body.getMessage().getText());
    response.setMessage(message);
    
    return Response.status(Response.Status.CREATED.getStatusCode()).entity(response).build();
  }
  
  private Response IdListCheck(RequestMessageBody body) {
    ResponseMessage response = new ResponseMessage();
    
    if(body.getIdList() == null || 0 == body.getIdList().size()
        || body.getMessage() == null
        || stringutil.isNullorEmpty(body.getMessage().getTitle())
        || stringutil.isNullorEmpty(body.getMessage().getText())
    ) {
      return  GenerateErrorRes(ErrorResponseEnum.E400);
    }
    return Response.status(Response.Status.CREATED.getStatusCode()).entity(response).build();
  }
  
  private Response EmptyidListCheck(int loopcnt, RequestMessageBody body) {
    ResponseMessage response = new ResponseMessage();
    
    if(stringutil.isNullorEmpty(body.getIdList().get(loopcnt).getId())) {
      return  GenerateErrorRes(ErrorResponseEnum.E400);
    }
    return Response.status(Response.Status.CREATED.getStatusCode()).entity(response).build();
    }  
  
  public Response GenerateDbConnectionGetDeviceTokenSelect(StringBuilder token, int loopcnt, RequestMessageBody body )  {
    ResponseMessage response = new ResponseMessage();
    String str = new String();
    
    try(Connection connection = ds.newConnection();
        PreparedStatement preparedStatement = connection.prepareStatement("SELECT DEVICE_TOKEN FROM T_TOKEN"
            + " WHERE USER_ID = ? ORDER BY UPDATE_DATE DESC LIMIT 1");){
                    preparedStatement.setString(1,  body.getIdList().get(loopcnt).getId());
        try(ResultSet resultset = preparedStatement.executeQuery();){
                           
                          if(resultset.next()) {
                             str =  resultset.getString("DEVICE_TOKEN");
                             token.append(str);
                         } else {
      
                         }
                           if( stringutil.isNullorEmpty(token.toString()) ) {
                             return Response.status(Response.Status.NOT_FOUND.getStatusCode()).entity(response).build();
                         }
                         }catch(SQLException e) {
                           e.printStackTrace();
                           return  GenerateErrorRes(ErrorResponseEnum.E500);
                       }    
                    } catch (SQLException e1) {
                      e1.printStackTrace();
                      return  GenerateErrorRes(ErrorResponseEnum.E500);
                    }
      return Response.status(Response.Status.CREATED.getStatusCode()).entity(response).build();
  }
  
  private Response GenerateDbConnectiontgetMessageSeqIDSelect(StringBuilder seqMID){
    
    ResponseMessage response = new ResponseMessage();
    String str = new String();
    try(Connection connection = ds.newConnection();
          PreparedStatement preparedStatement = connection.prepareStatement("SELECT NEXTVAL ('MessageSeqID')");
            ResultSet resultset = preparedStatement.executeQuery();){
            boolean br = false;
              br = resultset.next();
        
        Date date = new Date();
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        String strDate = dateFormat.format(date);
        str = strDate + String.format("%010d", resultset.getInt("nextval"));
        seqMID.append(str);
      }catch(SQLException e) {
        e.printStackTrace();
        return  GenerateErrorRes(ErrorResponseEnum.E500);
      }
    return Response.status(Response.Status.CREATED.getStatusCode()).entity(response).build();
  }
  
  private Response GenerateDbConnectioninsert(StringBuilder token, int loopcnt, RequestMessageBody body, StringBuilder seqMID) {
    
    ResponseMessage response = new ResponseMessage();

    int insertCount = 0;
   
    System.out.println(seqMID);
    try(Connection connection = ds.newConnection();
        PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO T_MESSAGE ("
            + "MESSAGE_SEQ_ID, "
            + "USER_ID, "
            + "SEND_TITLE, "
            + "SEND_MESSAGE, "
            + "STATUS, "
            + "CREATE_DATE, "
            + "UPDATE_DATE"
            + ") VALUES ("
            + "?, "
            + "?, "
            + "?, "
            + "?, "
            + "1, "
            + "current_timestamp, "
            + "current_timestamp)");){
      preparedStatement.setString(1,  seqMID.toString());
      preparedStatement.setString(2,  body.getIdList().get(loopcnt).getId());
      preparedStatement.setString(3, body.getMessage().getTitle());
      preparedStatement.setString(4, body.getMessage().getText());
      insertCount = preparedStatement.executeUpdate();
      
      if(insertCount != 1) {
        return  GenerateErrorRes(ErrorResponseEnum.E500);
      }
      
    }catch(SQLException e) {
      e.printStackTrace();
      return  GenerateErrorRes(ErrorResponseEnum.E500);
    }
    
    return Response.status(Response.Status.CREATED.getStatusCode()).entity(response).build();
  }
  
  private Response GenerateDbConnectionupdate(StringBuilder token, int loopcnt, StringBuilder seqMID, RequestMessageBody body){
    ResponseMessage response = new ResponseMessage();
    List<ResponseMessageBodyIDList> idList = new  ArrayList<>();
    
    int updateCount = 0;
    
    try(Connection connection = ds.newConnection();
        PreparedStatement preparedStatement = connection.prepareStatement("UPDATE T_MESSAGE "
            + "SET STATUS = 2, UPDATE_DATE = current_timestamp "
            + "WHERE MESSAGE_SEQ_ID = ?");){
      
      preparedStatement.setString(1,  seqMID.toString());
      
      updateCount = preparedStatement.executeUpdate();
      
      if(updateCount != 1) {
        return  GenerateErrorRes(ErrorResponseEnum.E500);
      }
      
    }catch(SQLException e) {
      e.printStackTrace();
      return  GenerateErrorRes(ErrorResponseEnum.E500);
    }
    ResponseMessageBodyIDList id = new ResponseMessageBodyIDList();
    id.setId(body.getIdList().get(loopcnt).getId());
    idList.add(id);
    return Response.status(Response.Status.CREATED.getStatusCode()).entity(response).build();
  }
  
  
  private Response GenerateFCMResponse(StringBuilder token, int loopcnt, StringBuilder seqMID,RequestMessageBody body) {
    ResponseMessage response = new ResponseMessage();
    
    PostLogicSendFcmSetBody plsb = new PostLogicSendFcmSetBody();
    String BODY = new String();
    BODY = plsb.SetBody(body, token.toString());
    int fcmstatus = 0;
    PostLogicSendFcm pls = new PostLogicSendFcm();
    fcmstatus = pls.sendfcm(BODY);
    if(200 == fcmstatus) {
      int updateCount = 0;
      
      try(Connection connection = ds.newConnection();
          PreparedStatement preparedStatement = connection.prepareStatement("UPDATE T_MESSAGE "
              + "SET STATUS = 2, UPDATE_DATE = current_timestamp "
              + "WHERE MESSAGE_SEQ_ID = ?");){
        
        preparedStatement.setString(1,  seqMID.toString());
        
        updateCount = preparedStatement.executeUpdate();
        
        if(updateCount != 1) {
            return  GenerateErrorRes(ErrorResponseEnum.E500);
        }
      }catch(SQLException e) {
        e.printStackTrace();
        return  GenerateErrorRes(ErrorResponseEnum.E500);
      }
    }else {
      int updateCount = 0;
      
      try(Connection connection = ds.newConnection();
          PreparedStatement preparedStatement = connection.prepareStatement("UPDATE T_MESSAGE "
              + "SET STATUS = 2, UPDATE_DATE = current_timestamp "
              + "WHERE MESSAGE_SEQ_ID = ?");){
        
        preparedStatement.setString(1,  seqMID.toString());
        
        updateCount = preparedStatement.executeUpdate();
        
        if(updateCount != 1) {
            return  GenerateErrorRes(ErrorResponseEnum.E500);
        }
        
      }catch(SQLException e) {
        e.printStackTrace();
        return  GenerateErrorRes(ErrorResponseEnum.E500);
      }
      return Response.status(fcmstatus).entity(response).build();
              }
    
    return Response.status(Response.Status.CREATED.getStatusCode()).entity(response).build();
  }
}


  
