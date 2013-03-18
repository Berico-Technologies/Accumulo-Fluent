package com.berico.accumulo;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;

public class RangeBuilder {
	
	Key start;
	Key end;
	
	boolean startKeyInclusive = true;
	boolean infiniteStartKey = false;
	boolean infiniteEndKey = false;
	boolean endKeyInclusive = true;
	boolean isPrefix = false;
	boolean isExact = false;
	
	public RangeBuilder(){}
	
	public RangeBuilder start(Key key){
		
		this.start = key;
		
		return this;
	}
	
	public RangeBuilder start(Key key, boolean inclusive){
		
		this.start = key;
		
		this.startKeyInclusive = inclusive;
		
		return this;
	}
	
	public RangeBuilder infiniteStart(){
		
		this.infiniteStartKey = true;
		
		return this;
	}
	
	public RangeBuilder end(Key key){
		
		this.end = key;
		
		return this;		
	}
	
	public RangeBuilder end(Key key, boolean inclusive){
		
		this.end = key;
		
		this.endKeyInclusive = inclusive;
		
		return this;		
	}
	
	public RangeBuilder infiniteEnd(){
		
		this.infiniteEndKey = true;
		
		return this;
	}
	
	public RangeBuilder exact(){
	
		isExact = true;
		
		return this;
	}
	
	public RangeBuilder prefix(){
		
		isPrefix = true;
		
		return this;
	}
	
	public static Range exact(Key key){
		
		RangeBuilder rb = new RangeBuilder();
		
		return rb.start(key).exact().build();
	}
	
	public static Range prefix(Key key){
		
		RangeBuilder rb = new RangeBuilder();
		
		return rb.start(key).prefix().build();
	}
	
	public Range build(){
		
		Range range = null;
		
		if (isPrefix){
			
			range = constructPrefix();
		}
		else if (isExact){
			
			range = constructExact();
		}
		else {
		
			if (infiniteStartKey || infiniteEndKey){
				
				range = new Range(start, end, startKeyInclusive, endKeyInclusive, infiniteStartKey, infiniteEndKey);
			}
			else {
				
				range = new Range(start, startKeyInclusive, end, endKeyInclusive);
			}
		}
		
		return range;
	}
	
	/**
	 * Construct a Prefix Range.
	 * @return Prefix Range.
	 */
	Range constructPrefix(){
		
		int score = score(this.start);
		
		// Prefix supports a max score of 13
		if (score > 13){ score = 13; }
		
		switch (score){
			case 1: return Range.prefix(this.start.getRow(), this.start.getColumnFamily());
			
			case 4: return Range.prefix(this.start.getRow(), this.start.getColumnFamily(), 
					this.start.getColumnQualifier());
			
			case 13: return Range.prefix(this.start.getRow(), this.start.getColumnFamily(), 
					this.start.getColumnQualifier(), this.start.getColumnVisibility());
			
			default : return Range.prefix(this.start.getRow());
		}
	}
	
	/**
	 * Construct an Exact Range.
	 * @return Exact Range.
	 */
	Range constructExact(){
		
		int score = score(this.start);
		
		// Prefix supports a max score of 13
		if (score > 13){ score = 13; }
		
		switch (score){
			case 1: return Range.exact(this.start.getRow(), this.start.getColumnFamily());
			
			case 4: return Range.exact(this.start.getRow(), this.start.getColumnFamily(), 
					this.start.getColumnQualifier());
			
			case 13: return Range.exact(this.start.getRow(), this.start.getColumnFamily(), 
					this.start.getColumnQualifier(), this.start.getColumnVisibility());
			
			case 31: return Range.exact(this.start.getRow(), this.start.getColumnFamily(), 
					this.start.getColumnQualifier(), this.start.getColumnVisibility(), this.start.getTimestamp());
			
			default : return Range.exact(this.start.getRow());
		}
	}
	
	int score(Key key){
		
		int score = 0;
		
		if (key.getColumnFamily() != null){
			
			score += 1;
		}
		
		if (key.getColumnQualifier() != null){
			
			score += 3;
		}
		
		if (key.getColumnVisibility() != null){
			
			score += 9;
		}
		
		if (key.getTimestamp() != Integer.MAX_VALUE){
			
			score += 18;
		}
		
		return score;
	}
}