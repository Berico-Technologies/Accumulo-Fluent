package com.berico.accumulo;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

public class CellValueRetrievalOpsIterator implements Iterator<CellValueRetrievalOps> {

	
	Iterator<Entry<Key, Value>> decoratedIterator;
	ScanOps parent;
	
	public CellValueRetrievalOpsIterator(ScanOps parent, Iterator<Entry<Key, Value>> iterator){
		
		this.parent = parent;
		this.decoratedIterator = iterator;
	}
	
	@Override
	public boolean hasNext() {
		
		return this.decoratedIterator.hasNext();
	}

	@Override
	public CellValueRetrievalOps next() {
		
		Entry<Key, Value> next = this.decoratedIterator.next();
		
		return new CellValueRetrievalOps(this.parent, next);
	}

	@Override
	public void remove() {
		
		this.decoratedIterator.remove();
	}
}