package me.ghector

import org.apache.cassandra.thrift.Cassandra 
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.thrift.protocol.TBinaryProtocol 
import org.apache.thrift.protocol.TProtocol 
import org.apache.thrift.transport.TSocket 
import org.apache.thrift.transport.TTransport 

class GHector {
	def server, port;
	def defConLevel = ConsistencyLevel.QUORUM
	
	def getTimestamp() {
		new Date().getTime();
	}
	
	def put(ctx,value) {
		execute { Cassandra.Client client ->
			use (CassandraCategory) {
				client.insert ctx.ks, ctx.key, cpath(ctx), value.serialize(), timestamp, defConLevel
			}
		}
	}
	
	def get(ctx) {
		execute { Cassandra.Client client ->
			use (CassandraCategory) {
				if (ctx['request_type'] == "col_slice") {
					return client.get_range_slices(ctx.ks, cparent(ctx), predicate(ctx), range(ctx), defConLevel).deserialize()
				} else {
					return client.get(ctx.ks, ctx.key, cpath(ctx), defConLevel).deserialize()
				}
			}
		}
	}
	
	def execute(cmd) {
		TTransport tt = new TSocket(server, port);
		TProtocol tp =  new TBinaryProtocol(tt);
		Cassandra.Client c = new Cassandra.Client(tp);
		try {
			tt.open();
			cmd(c);
		} finally {
			tt.close();
		}
	}
}
