package com.berico.accumulo;

import org.apache.accumulo.core.data.Key;

public class KeyBuilder {

	String rowId = null;
	String columnFamily = null;
	String columnQualifier = null;
	String columnVisibility = null;
	long   timestamp = 0;

	int configuration = 0;
	
	public KeyBuilder(){}
	
	public KeyBuilder row(String key){
		
		this.rowId = key;
		
		return this;
	}
	
	public KeyBuilder family(String family){
		
		this.columnFamily = family;
		
		configuration += 1;
		
		return this;
	}
	
	public KeyBuilder column(String column){
		
		this.columnQualifier = column;
		
		configuration += 3;
		
		return this;
	}
	
	public KeyBuilder visibility(String visibility){
		
		this.columnVisibility = visibility;
		
		configuration += 9;
		
		return this;
	}
	
	public KeyBuilder timestamp(long timestamp){
		
		this.timestamp = timestamp;
		
		configuration += 18;
		
		return this;
	}
	
	public Key build(){
		
		switch(configuration){
			case 1:  return new Key(rowId, columnFamily);
			case 4:  return new Key(rowId, columnFamily, columnQualifier);
			case 13: return new Key(rowId, columnFamily, columnQualifier, columnVisibility);
			case 22: return new Key(rowId, columnFamily, columnQualifier, timestamp);
			case 31: return new Key(rowId, columnFamily, columnQualifier, columnVisibility, timestamp);
			default: return new Key(rowId);
		}
	}
}
