package com.berico.accumulo;

import java.nio.ByteBuffer;

public class ConversionUtils {
	
	/**
	 * Convert a double to a byte array.
	 * @param value Double to convert.
	 * @return Byte array value.
	 */
	public static byte[] toByteArray(double value) {
		
	    byte[] bytes = new byte[8];
	    
	    ByteBuffer.wrap(bytes).putDouble(value);
	    
	    return bytes;
	}
	
	/**
	 * Convert an int to a byte array.
	 * @param value Integer to convert.
	 * @return Byte array value.
	 */
	public static byte[] toByteArray(int value) {
		
	    byte[] bytes = new byte[4];
	    
	    ByteBuffer.wrap(bytes).putInt(value);
	    
	    return bytes;
	}
	
	/**
	 * Convert a long to a byte array.
	 * @param value Long to convert.
	 * @return Byte array value.
	 */
	public static byte[] toByteArray(long value) {
		
	    byte[] bytes = new byte[8];
	    
	    ByteBuffer.wrap(bytes).putLong(value);
	    
	    return bytes;
	}

	/**
	 * Convert a boolean to a byte array.
	 * @param value Boolean to convert.
	 * @return Byte array value.
	 */
	public static byte[] toByteArray(boolean value) {

	    int intValue = (value)? 1 : 0;
	    
	    return toByteArray(intValue);
	}
	
	
	/**
	 * Convert a byte array to a double.
	 * @param bytes Bytes to convert.
	 * @return Double value.
	 */
	public static double toDouble(byte[] bytes) {
		
	    return ByteBuffer.wrap(bytes).getDouble();
	}
	
	/**
	 * Convert a byte array to an integer.
	 * @param bytes Bytes to convert.
	 * @return Integer value.
	 */
	public static int toInt(byte[] bytes) {
		
	    return ByteBuffer.wrap(bytes).getInt();
	}
	
	/**
	 * Convert a byte array to a boolean.
	 * @param bytes Bytes to convert.
	 * @return Boolean value.
	 */
	public static boolean toBoolean(byte[] bytes) {
		
	    int value = ByteBuffer.wrap(bytes).getInt();
	    
	    return (value > 0) ? true : false;
	}

	/**
	 * Convert a byte array to a long.
	 * @param bytes Bytes to convert.
	 * @return Long value.
	 */
	public static double toLong(byte[] bytes) {
		
	    return ByteBuffer.wrap(bytes).getLong();
	}
}
