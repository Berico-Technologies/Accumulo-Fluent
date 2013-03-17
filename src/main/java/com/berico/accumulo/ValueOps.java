package com.berico.accumulo;

import static com.berico.accumulo.ConversionUtils.toByteArray;

import java.math.BigInteger;
import java.util.Date;
import java.util.UUID;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

public class ValueOps {
	
	RowMultiOps parent;
	ColumnIdentifiers columnIdentifiers;
	
	public ValueOps(ColumnIdentifiers columnIdentifiers, RowMultiOps parent){
		
		this.columnIdentifiers = columnIdentifiers;
		this.parent = parent;
	}
	
	/* ####### Value Types ############################################################### */
	
	public RowMultiOps value(String value){
		
		return value(value, 0);
	}
	
	public RowMultiOps value(String value, long timestamp){
		
		return handleValue(toByteArray(value), timestamp);
	}
	
	public RowMultiOps value(int value){
		
		return value(value, 0);
	}
	
	public RowMultiOps value(int value, long timestamp){
		
		return handleValue(toByteArray(value), timestamp);
	}
	
	public RowMultiOps value(double value){
		
		return value(value, 0);
	}
	
	public RowMultiOps value(double value, long timestamp){
		
		return handleValue(toByteArray(value), timestamp);
	}
	
	public RowMultiOps value(boolean value){
		
		return value(value, 0);
	}
	
	public RowMultiOps value(boolean value, long timestamp){
		
		return handleValue(toByteArray(value), timestamp);
	}
	
	public RowMultiOps value(long value){
		
		return value(value, 0);
	}
	
	public RowMultiOps value(long value, long timestamp){
		
		return handleValue(toByteArray(value), timestamp);
	}
	
	public RowMultiOps value(Date value){
		
		return value(value, 0);
	}
	
	public RowMultiOps value(Date value, long timestamp){
		
		return handleValue(toByteArray(value), timestamp);
	}
	
	public RowMultiOps value(BigInteger value){
		
		return value(value, 0);
	}
	
	public RowMultiOps value(BigInteger value, long timestamp){
		
		return handleValue(toByteArray(value), timestamp);
	}
	
	public RowMultiOps value(UUID value){
		
		return value(value, 0);
	}
	
	public RowMultiOps value(UUID value, long timestamp){
		
		return handleValue(toByteArray(value), timestamp);
	}
	
	/* ####### Helpers ################################################################### */
	
	RowMultiOps handleValue(byte[] value, long timestamp){
		
		Text cf = new Text(columnIdentifiers.getColumnFamily());
		Text cq = new Text(columnIdentifiers.getColumnQualifier());
		Value v = new Value(value);
		
		if (columnIdentifiers.hasVisibilityExpression() && timestamp != 0){
			
			this.parent.mutation.put(cf, cq, 
				new ColumnVisibility(columnIdentifiers.getVisibilityExpression()), timestamp, v);
		}
		else if (columnIdentifiers.hasVisibilityExpression()){
			
			this.parent.mutation.put(cf, cq, 
				new ColumnVisibility(columnIdentifiers.getVisibilityExpression()), v);
		}
		else if (timestamp != 0){
			
			this.parent.mutation.put(cf, cq, timestamp, v);
		}
		else {
			
			this.parent.mutation.put(cf, cq, v);
		}
		
		return parent;
	}
}