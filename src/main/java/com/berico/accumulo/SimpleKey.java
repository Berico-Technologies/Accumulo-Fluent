package com.berico.accumulo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.thrift.TKey;
import org.apache.hadoop.io.Text;

public class SimpleKey extends Key {

	Key decoratedKey;
	
	public SimpleKey(Key decoratedKey){
		
		this.decoratedKey = decoratedKey;
	}
	
	public String rowKey(){
		
		return this.decoratedKey.getRow().toString();
	}
	
	public String family(){
		
		return this.decoratedKey.getColumnFamily().toString();
	}
	
	public String qualifier(){
		
		return this.decoratedKey.getColumnQualifier().toString();
	}
	
	public String visibility(){
		
		return this.decoratedKey.getColumnVisibility().toString();
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException {
		
		return this.decoratedKey.clone();
	}

	@Override
	public int compareColumnFamily(Text cf) {
		
		return this.decoratedKey.compareColumnFamily(cf);
	}

	@Override
	public int compareColumnQualifier(Text cq) {
		
		return this.decoratedKey.compareColumnQualifier(cq);
	}

	@Override
	public int compareRow(Text r) {
		
		return this.decoratedKey.compareRow(r);
	}

	@Override
	public int compareTo(Key other, PartialKey part) {
		
		return this.decoratedKey.compareTo(other, part);
	}

	@Override
	public int compareTo(Key other) {
		
		return this.decoratedKey.compareTo(other);
	}
	
	@Override
	public boolean equals(Key other, PartialKey part) {
		
		return this.decoratedKey.equals(other, part);
	}

	@Override
	public boolean equals(Object o) {
		
		return this.decoratedKey.equals(o);
	}

	@Override
	public Key followingKey(PartialKey part) {
		
		return this.decoratedKey.followingKey(part);
	}

	@Override
	public Text getColumnFamily() {
		
		return this.decoratedKey.getColumnFamily();
	}

	@Override
	public Text getColumnFamily(Text cf) {
		
		return this.decoratedKey.getColumnFamily(cf);
	}

	@Override
	public ByteSequence getColumnFamilyData() {
		
		return this.decoratedKey.getColumnFamilyData();
	}

	@Override
	public Text getColumnQualifier() {
		
		return this.decoratedKey.getColumnQualifier();
	}

	@Override
	public Text getColumnQualifier(Text cq) {
		
		return this.decoratedKey.getColumnQualifier(cq);
	}

	@Override
	public ByteSequence getColumnQualifierData() {
		
		return this.decoratedKey.getColumnQualifierData();
	}

	@Override
	public ByteSequence getColumnVisibilityData() {
		
		return this.decoratedKey.getColumnVisibilityData();
	}

	@Override
	public int getLength() {
		
		return this.decoratedKey.getLength();
	}

	@Override
	public Text getRow() {
		
		return this.decoratedKey.getRow();
	}

	@Override
	public Text getRow(Text r) {
		
		return this.decoratedKey.getRow(r);
	}

	@Override
	public ByteSequence getRowData() {
		
		return this.decoratedKey.getRowData();
	}

	@Override
	public int getSize() {
		
		return this.decoratedKey.getSize();
	}

	@Override
	public long getTimestamp() {
		
		return this.decoratedKey.getTimestamp();
	}

	@Override
	public int hashCode() {
		
		return this.decoratedKey.hashCode();
	}

	@Override
	public boolean isDeleted() {
		
		return this.decoratedKey.isDeleted();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		this.decoratedKey.readFields(in);
	}

	@Override
	public void set(Key k) {
		
		this.decoratedKey.set(k);
	}

	@Override
	public void setDeleted(boolean deleted) {
		
		this.decoratedKey.setDeleted(deleted);
	}

	@Override
	public void setTimestamp(long ts) {
		
		this.decoratedKey.setTimestamp(ts);
	}

	@Override
	public String toString() {
		
		return this.decoratedKey.toString();
	}

	@Override
	public String toStringNoTime() {
		
		return this.decoratedKey.toStringNoTime();
	}

	@Override
	public TKey toThrift() {
		
		return this.decoratedKey.toThrift();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		this.decoratedKey.write(out);
	}
}
