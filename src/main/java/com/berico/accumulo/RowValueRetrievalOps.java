package com.berico.accumulo;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

public class RowValueRetrievalOps {

	ScanOps parent;
	Iterator<Entry<Key, Value>> row;
	
	public RowValueRetrievalOps(ScanOps parent, Iterator<Entry<Key, Value>> row){
		
		this.parent = parent;
		this.row = row;
	}
	
	public Iterator<CellValueRetrievalOps> iterator(){
		
		return new CellValueRetrievalOpsIterator(this.parent, row);
	}
	
}