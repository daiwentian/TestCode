package com.open.jp.notification.util;

public class StringUtil {

	private String str;
	
	public StringUtil() {
		
	}

	public String getStr() {
		return str;
	}

	public void setStr(String str) {
		this.str = str;
	}
	
	public boolean isNullorEmpty(String str) {
		
		return (str == null || 0 == str.length());
	}
}

