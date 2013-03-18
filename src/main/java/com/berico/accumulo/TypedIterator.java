package com.berico.accumulo;

import java.util.Map.Entry;
import java.util.AbstractMap.SimpleEntry;
import java.util.Iterator;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.typo.encoders.Lexicoder;

public class TypedIterator<V> implements Iterator<Entry<Key, V>> {

	Lexicoder<V> lexicoder;
	Iterator<Entry<Key, Value>> decoratedIterator;
	
	public TypedIterator(Iterator<Entry<Key, Value>> iterator, Lexicoder<V> lexicoder){
		
		this.lexicoder = lexicoder;
		this.decoratedIterator = iterator;
	}
	
	@Override
	public boolean hasNext() {
		
		return this.decoratedIterator.hasNext();
	}
	
	@Override
	public Entry<Key, V> next() {
		
		Entry<Key, Value> next = this.decoratedIterator.next();
		
		return new SimpleEntry<Key, V>(
			next.getKey(), lexicoder.decode(next.getValue().get()));
	}
	
	public Entry<SimpleKey, V> nextEntry(){
		
		Entry<Key, Value> next = this.decoratedIterator.next();
		
		return new SimpleEntry<SimpleKey, V>(
			new SimpleKey(next.getKey()), lexicoder.decode(next.getValue().get()));
	}

	@Override
	public void remove() {
		
		this.decoratedIterator.remove();
	}
}
