package com.open.jp.async.logic;

import javax.ws.rs.core.Response;

public enum ErrorResponseEnum {
	//エラーステータスコード400を返す。
	  E400(Response.Status.BAD_REQUEST.getStatusCode(), "E400", "パラメータ形式が異なります。"),
	  //エラーステータスコード500を返す。
	  E500(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), "E500", "Kafkaの接続に失敗しました。");

	  private int ErrorStatus;

	  private String ErrorStatusCodeMessage;

	  private String ErrorMessage;

	  private ErrorResponseEnum(int ErrorStatus, String ErrorStatusCodeMessage, String ErrorMessage) {
	    this.ErrorStatus = ErrorStatus; //ステータスコード
	    this.ErrorStatusCodeMessage = ErrorStatusCodeMessage; //ステータスコードメッセージ（E400, E409, E500)
	    this.ErrorMessage = ErrorMessage; //エラーメッセージの出力
	  }

	  public int getErrorStatus() {
	    return ErrorStatus;
	  }

	  public String getErrorStatusCodeMessage() {
	    return ErrorStatusCodeMessage;
	  }

	  public String getErrorMessage() {
	    return ErrorMessage;
	  }

}

