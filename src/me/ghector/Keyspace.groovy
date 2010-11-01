package me.ghector

import xebia.cassandraclient.query.Selector;

class Keyspace {
	def String ks
	def String cf // not used 
	def GHector client
	
	def getAt(String key) {
		def ctx = Selector.readContext(key)
		ctx["ks"] = ks;
		def ret = client.get( ctx);
		println ret
		return ret
	}
	
	void putAt(String key, Object value) {
		def ctx = Selector.writeContext(key)
		ctx["ks"] = ks;
		client.put( ctx, value.toString());
	}
	
	def count(String key) {
		def selector = Selector.readContext(key)
	}
	
	def getSlice(key) {
		def selector = Selector.readContext(key)
		//return client.getSlice(ks, selector.cf, selector.key, selector.column)
	}
	
	def truncate() {
		//client.truncate ks, "Users"
	}
}
