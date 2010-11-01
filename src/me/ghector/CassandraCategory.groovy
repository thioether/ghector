package me.ghector

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;

class CassandraCategory {
	
	static cpath(ks, args) {
		ColumnPath cp = new ColumnPath(args["cf"]);
		cp.setColumn(args["col"].bytes);
		return cp;
	}
	
	static serialize(String s) {
		return s.getBytes("UTF-8");
	}
	
	static deserialize(ColumnOrSuperColumn csc) {
		if (csc.isSetSuper_column()) {
			def sc = csc.super_column;
			//TODO
		} else {
			def c = csc.column;
			return new String(c.value,"UTF-8");
		}
	}
	
	static range(ks,ctx) {
		KeyRange kr = new KeyRange();
		kr.start_key = ctx['key_start'] 
		kr.end_key = ctx['key_end']
		return kr
	}
	
	static cparent(ks,ctx) {
		ColumnParent cp = new ColumnParent(ctx['cf']);
		return cp
		// TOD handle super ...
	}
	
	static predicate(ks,ctx) {
		SlicePredicate sp = new SlicePredicate()
		println ctx['cols']
		if (ctx['cols'] != null) {
			ctx['cols'].each { 
				sp.addToColumn_names it.bytes
			}
		} else {
			SliceRange sr = new SliceRange();
			sr.start = ctx['col_start'].bytes
			sr.finish = ctx['col_end'].bytes
			sp.slice_range = sr
		}
		return sp
	}
	
	static deserialize(obj) {
		def keyToValue = [:]
		obj.each { 
			def retValue = [];
			it.columns.each {
				retValue << deserialize(it);
			}
			keyToValue[it.key] = retValue;
		}
		println keyToValue;
		return keyToValue;
	}
}
