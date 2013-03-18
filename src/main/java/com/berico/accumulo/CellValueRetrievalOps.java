package com.berico.accumulo;

import java.math.BigInteger;
import java.util.AbstractMap.SimpleEntry;
import java.util.Date;
import java.util.Map.Entry;
import java.util.UUID;

import javax.activation.UnsupportedDataTypeException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

public class CellValueRetrievalOps {

	public interface ValueHandler<T> {
		
		public void handle(T value);
	}
	
	Entry<Key, Value> cell;
	ScanOps parent;
	
	public CellValueRetrievalOps(ScanOps parent, Entry<Key, Value> cell){
		
		this.cell = cell;
		this.parent = parent;
	}
	
	public Entry<SimpleKey, Value> get(){
	
		return new SimpleEntry<SimpleKey, Value>(new SimpleKey(cell.getKey()), cell.getValue());
	}
	
	public <T> Entry<SimpleKey, T> get(Class<T> clazz) throws UnsupportedDataTypeException{
		
		T decodedValue = ConversionUtils.convert(cell.getValue().get(), clazz);
		
		return new SimpleEntry<SimpleKey, T>(new SimpleKey(cell.getKey()), decodedValue);
	}
	
	public SimpleKey key(){
		
		return new SimpleKey(cell.getKey());
	}
	
	public ScanOps get(ValueHandler<Entry<SimpleKey, Value>> handler){
		
		handler.handle(get());
		
		return this.parent;
	}
	
	public <T> ScanOps get(ValueHandler<Entry<SimpleKey, T>> handler, Class<T> clazz) throws UnsupportedDataTypeException{
		
		handler.handle(get(clazz));
		
		return this.parent;
	}
	
	public int asInt(){
		
		return ConversionUtils.toInt(cell.getValue().get());
	}
	
	public ScanOps asInt(ValueHandler<Integer> handler){
		
		handler.handle(asInt());
		
		return this.parent;
	}
	
	public double asDouble(){
		
		return ConversionUtils.toDouble(cell.getValue().get());
	}
	
	public ScanOps asDouble(ValueHandler<Double> handler){
		
		handler.handle(asDouble());
		
		return this.parent;
	}
	
	public long asLong(){
			
		return ConversionUtils.toLong(cell.getValue().get());
	}
	
	public ScanOps asLong(ValueHandler<Long> handler){
		
		handler.handle(asLong());
		
		return this.parent;
	}
	
	public BigInteger asBigInteger(){
		
		return ConversionUtils.toBigInteger(cell.getValue().get());
	}
	
	public ScanOps asBigInteger(ValueHandler<BigInteger> handler){
		
		handler.handle(asBigInteger());
		
		return this.parent;
	}
	
	public byte[] asBytes(){
		
		return cell.getValue().get();
	}
	
	public ScanOps asBytes(ValueHandler<byte[]> handler){
		
		handler.handle(asBytes());
		
		return this.parent;
	}

	public String asString(){
		
		return ConversionUtils.toString(cell.getValue().get());
	}
	
	public ScanOps asString(ValueHandler<String> handler){
		
		handler.handle(asString());
		
		return this.parent;
	}
	
	public Date asDate(){
		
		return ConversionUtils.toDate(cell.getValue().get());
	}
	
	public ScanOps asDate(ValueHandler<Date> handler){
		
		handler.handle(asDate());
		
		return this.parent;
	}
	
	public UUID asUUID(){
		
		return ConversionUtils.toUUID(cell.getValue().get());
	}

	public ScanOps asUUID(ValueHandler<UUID> handler){
		
		handler.handle(asUUID());
		
		return this.parent;
	}
}
