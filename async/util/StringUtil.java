package com.open.jp.async.util;

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
		// strがnullもしくは空文字であればtrueを返す
		return (str == null || 0 == str.length());
	}
}

