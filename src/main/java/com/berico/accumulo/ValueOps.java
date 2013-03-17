package com.berico.accumulo;

import static com.berico.accumulo.ConversionUtils.toByteArray;

import java.math.BigInteger;
import java.util.Date;
import java.util.UUID;

import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

public class ValueOps<T> {
	
	public interface CompletionHandler {
		
		public void complete(Mutation mutation) throws MutationsRejectedException;
	}
	
	T parent;
	ColumnIdentifiers columnIdentifiers;
	Mutation mutation;
	CompletionHandler onComplete;
	
	public ValueOps(ColumnIdentifiers columnIdentifiers, Mutation mutation, T parent, CompletionHandler onComplete){
		
		this.columnIdentifiers = columnIdentifiers;
		this.parent = parent;
		this.mutation = mutation;
		this.onComplete = onComplete;
	}
	
	/* ####### Value Types ############################################################### */
	
	public T value(String value) throws MutationsRejectedException {
		
		return value(value, 0);
	}
	
	public T value(String value, long timestamp) throws MutationsRejectedException {
		
		return handleValue(toByteArray(value), timestamp);
	}
	
	public T value(int value) throws MutationsRejectedException {
		
		return value(value, 0);
	}
	
	public T value(int value, long timestamp) throws MutationsRejectedException {
		
		return handleValue(toByteArray(value), timestamp);
	}
	
	public T value(double value) throws MutationsRejectedException {
		
		return value(value, 0);
	}
	
	public T value(double value, long timestamp) throws MutationsRejectedException {
		
		return handleValue(toByteArray(value), timestamp);
	}
	
	public T value(boolean value) throws MutationsRejectedException {
		
		return value(value, 0);
	}
	
	public T value(boolean value, long timestamp) throws MutationsRejectedException {
		
		return handleValue(toByteArray(value), timestamp);
	}
	
	public T value(long value) throws MutationsRejectedException {
		
		return value(value, 0);
	}
	
	public T value(long value, long timestamp) throws MutationsRejectedException {
		
		return handleValue(toByteArray(value), timestamp);
	}
	
	public T value(Date value) throws MutationsRejectedException {
		
		return value(value, 0);
	}
	
	public T value(Date value, long timestamp) throws MutationsRejectedException {
		
		return handleValue(toByteArray(value), timestamp);
	}
	
	public T value(BigInteger value) throws MutationsRejectedException {
		
		return value(value, 0);
	}
	
	public T value(BigInteger value, long timestamp) throws MutationsRejectedException {
		
		return handleValue(toByteArray(value), timestamp);
	}
	
	public T value(UUID value) throws MutationsRejectedException {
		
		return value(value, 0);
	}
	
	public T value(UUID value, long timestamp) throws MutationsRejectedException{
		
		return handleValue(toByteArray(value), timestamp);
	}
	
	/* ####### Helpers ################################################################### */
	
	T handleValue(byte[] value, long timestamp) throws MutationsRejectedException {
		
		Text cf = new Text(columnIdentifiers.getColumnFamily());
		Text cq = new Text(columnIdentifiers.getColumnQualifier());
		Value v = new Value(value);
		
		if (columnIdentifiers.hasVisibilityExpression() && timestamp != 0){
			
			this.mutation.put(cf, cq, 
				new ColumnVisibility(columnIdentifiers.getVisibilityExpression()), timestamp, v);
		}
		else if (columnIdentifiers.hasVisibilityExpression()){
			
			this.mutation.put(cf, cq, 
				new ColumnVisibility(columnIdentifiers.getVisibilityExpression()), v);
		}
		else if (timestamp != 0){
			
			this.mutation.put(cf, cq, timestamp, v);
		}
		else {
			
			this.mutation.put(cf, cq, v);
		}
		
		onComplete.complete(mutation);
		
		return parent;
	}
}