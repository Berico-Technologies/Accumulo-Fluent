package com.berico.accumulo;

import java.math.BigInteger;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.activation.UnsupportedDataTypeException;

import org.apache.accumulo.typo.encoders.BigIntegerLexicoder;
import org.apache.accumulo.typo.encoders.BytesLexicoder;
import org.apache.accumulo.typo.encoders.DateLexicoder;
import org.apache.accumulo.typo.encoders.DoubleLexicoder;
import org.apache.accumulo.typo.encoders.IntegerLexicoder;
import org.apache.accumulo.typo.encoders.Lexicoder;
import org.apache.accumulo.typo.encoders.LongLexicoder;
import org.apache.accumulo.typo.encoders.StringLexicoder;
import org.apache.accumulo.typo.encoders.UUIDLexicoder;

/**
 * A set of functions to convert Java primitives/objects into byte arrays,
 * and vice versa.
 * 
 * @author Richard Clayton (Berico Technologies)
 */
public class ConversionUtils {
	
	/**
	 * Lexicoders from the Typo library that will perform the conversion.
	 * These objects keep no state, so it's safe to instantiate them once,
	 * rather than creating unnecessary instances on each conversion.
	 */
	static IntegerLexicoder intLex = new IntegerLexicoder();
	static DoubleLexicoder doubleLex = new DoubleLexicoder();
	static LongLexicoder longLex = new LongLexicoder();
	static StringLexicoder stringLex = new StringLexicoder();
	static BigIntegerLexicoder bigintLex = new BigIntegerLexicoder();
	static DateLexicoder dateLex = new DateLexicoder();
	static UUIDLexicoder uuidLex = new UUIDLexicoder();
	static BytesLexicoder bytesLex = new BytesLexicoder();
	
	/**
	 * Registry of Lexicoders, index by the class type the support.
	 */
	static Map<Class<?>, Lexicoder<?>> lexicoderRegistry = new HashMap<Class<?>, Lexicoder<?>>();
	
	/**
	 * Initialize the registry.
	 */
	static {
		
		lexicoderRegistry.put(Integer.class, intLex);
		lexicoderRegistry.put(Double.class, doubleLex);
		lexicoderRegistry.put(Long.class, longLex);
		lexicoderRegistry.put(String.class, stringLex);
		lexicoderRegistry.put(BigInteger.class, bigintLex);
		lexicoderRegistry.put(Date.class, dateLex);
		lexicoderRegistry.put(UUID.class, uuidLex);
		lexicoderRegistry.put(byte[].class, bytesLex);
		
	}
	
	/**
	 * Generic facility for converting bytes to desired type via Lexicoder.
	 * @param bytes Bytes to decode.
	 * @param clazz Desired class type.
	 * @return instance of the desired type represented by the byte array.
	 * @throws UnsupportedDataTypeException Thrown if there is not a Lexicoder that
	 * supports the submitted class type.
	 */
	public static <T> T convert(byte[] bytes, Class<T> clazz) throws UnsupportedDataTypeException {
		
		if (!lexicoderRegistry.containsKey(clazz)){
			throw new UnsupportedDataTypeException(
				String.format("%s is not a supported conversion type.", clazz.getCanonicalName()));
		}
		
		@SuppressWarnings("unchecked")
		Lexicoder<T> lexicoder = (Lexicoder<T>) lexicoderRegistry.get(clazz);
		
		return lexicoder.decode(bytes);
	}
	
	/**
	 * Convert a BigInteger to a byte array.
	 * @param value BigInteger to convert.
	 * @return Byte array value.
	 */
	public static byte[] toByteArray(BigInteger value){
		
		return bigintLex.encode(value);
	}
	
	/**
	 * Convert a Date to a byte array.
	 * @param value Date to convert.
	 * @return Byte array value.
	 */
	public static byte[] toByteArray(Date value){
		
		return dateLex.encode(value);
	}
	
	/**
	 * Convert a UUID to a byte array.
	 * @param value UUID to convert.
	 * @return Byte array value.
	 */
	public static byte[] toByteArray(UUID value){
		
		return uuidLex.encode(value);
	}
	
	/**
	 * Convert a String to a byte array.
	 * @param value String to convert.
	 * @return Byte array value.
	 */
	public static byte[] toByteArray(String value){
		
		return stringLex.encode(value);
	}
	
	/**
	 * Convert a double to a byte array.
	 * @param value Double to convert.
	 * @return Byte array value.
	 */
	public static byte[] toByteArray(double value) {
		
	    return doubleLex.encode(value);
	}
	
	/**
	 * Convert an int to a byte array.
	 * @param value Integer to convert.
	 * @return Byte array value.
	 */
	public static byte[] toByteArray(int value) {
		
		return intLex.encode(value);
	}
	
	/**
	 * Convert a long to a byte array.
	 * @param value Long to convert.
	 * @return Byte array value.
	 */
	public static byte[] toByteArray(long value) {
		
	    return longLex.encode(value);
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
	 * Convert a byte array to a BigInteger.
	 * @param bytes Bytes to convert.
	 * @return BigInteger value.
	 */
	public static BigInteger toBigInteger(byte[] bytes){
		
		return bigintLex.decode(bytes);
	}
	
	/**
	 * Convert a byte array to a Date.
	 * @param bytes Bytes to convert.
	 * @return Date value.
	 */
	public static Date toDate(byte[] bytes){
		
		return dateLex.decode(bytes);
	}
	
	/**
	 * Convert a byte array to a UUID.
	 * @param bytes Bytes to convert.
	 * @return UUID value.
	 */
	public static UUID toUUID(byte[] bytes){
		
		return uuidLex.decode(bytes);
	}
	
	/**
	 * Convert a byte array to a String.
	 * @param bytes Bytes to convert.
	 * @return String value.
	 */
	public static String toString(byte[] bytes){
		
		return stringLex.decode(bytes);
	}
	
	/**
	 * Convert a byte array to a double.
	 * @param bytes Bytes to convert.
	 * @return Double value.
	 */
	public static double toDouble(byte[] bytes) {
		
	    return doubleLex.decode(bytes);
	}
	
	/**
	 * Convert a byte array to an integer.
	 * @param bytes Bytes to convert.
	 * @return Integer value.
	 */
	public static int toInt(byte[] bytes) {
		
	    return intLex.decode(bytes);
	}
	
	/**
	 * Convert a byte array to a boolean.
	 * @param bytes Bytes to convert.
	 * @return Boolean value.
	 */
	public static boolean toBoolean(byte[] bytes) {
		
	    int value = toInt(bytes);
	    
	    return (value > 0) ? true : false;
	}

	/**
	 * Convert a byte array to a long.
	 * @param bytes Bytes to convert.
	 * @return Long value.
	 */
	public static long toLong(byte[] bytes) {
		
	    return longLex.decode(bytes);
	}
	
}
