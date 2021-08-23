package com.open.jp.notification.logic;

import javax.ws.rs.core.Response;

public enum ErrorResponseEnum {
	  E400(Response.Status.BAD_REQUEST                     .getStatusCode(), "E400", "パラメータ形式が異なります。"),
	  E404(Response.Status.NOT_FOUND                         .getStatusCode(), "E404", "IDが存在しません。"),
	  E409POST(Response.Status.CONFLICT                              .getStatusCode(), "E409", "idが既に存在します。"),
	  E409PUT(Response.Status.CONFLICT                            .getStatusCode(), "E409", "レコードが更新できません。"),
	  E409DEL(Response.Status.CONFLICT                            .getStatusCode(), "E409", "レコードが削除できません。"),
	  E500(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), "E500", "DBサーバの接続に失敗しました。");

	  private int       ErrorStatus;

	  private String ErrorStatusCodeMessage;

	  private String ErrorMessage;

	  private ErrorResponseEnum(int ErrorStatus, String ErrorStatusCodeMessage, String ErrorMessage) {
	    this.ErrorStatus = ErrorStatus; 
	    this.ErrorStatusCodeMessage = ErrorStatusCodeMessage; 
	    this.ErrorMessage = ErrorMessage; 
	  }

	  public int      getErrorStatus() {
	    return ErrorStatus;
	  }

	  public String getErrorStatusCodeMessage() {
	    return ErrorStatusCodeMessage;
	  }

	  public String getErrorMessage() {
	    return ErrorMessage;
	  }

}

