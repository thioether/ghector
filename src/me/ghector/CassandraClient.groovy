package me.ghector

import java.util.Date;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.Deletion;

class CassandraClient {
	
	def String server
	def int port
	
	def getKeyspace(keyspace, cf) {
		return new Keyspace(ks:keyspace,client:this,cf:cf);
	}
	
	def put(keyspace,cf,cn,key,value) {
		execute { Cassandra.Client client ->
			ColumnPath cp = new ColumnPath(cf);
			cp.setColumn(cn.getBytes())
			client.insert keyspace, key  ,cp , value.getBytes(), new Date().getTime(), ConsistencyLevel.QUORUM
		}
	}
	
	def get(keyspace,cf,cn, key) {
		execute { Cassandra.Client client ->
			ColumnPath cp = new ColumnPath(cf);
			cp.setColumn(cn.getBytes())
			client.get keyspace, key, cp, ConsistencyLevel.QUORUM
		}
	}
	
	def count(keyspace,cf,cn, key) {
		execute { Cassandra.Client client->
			ColumnParent cp = new ColumnParent(cf);
			client.get_count keyspace, key, cp, ConsistencyLevel.QUORUM
		}
	}
	
	def getSlice(keyspace,cf,key,cn) {
		execute { Cassandra.Client client->
			ColumnParent cp = new ColumnParent(cf);
			SlicePredicate sp = new SlicePredicate();
			//sp.column_names = [cn.getBytes()];
			SliceRange sr = new SliceRange();
			sr.setCount 100
			sr.setStart "".bytes
			sr.setFinish("".bytes)
			sp.setSlice_range sr
			client.get_slice	 keyspace, key, cp, sp, ConsistencyLevel.QUORUM
		}
	}
	
	def truncate(keyspace,cf) {
		// This is what is already in 0.7 ... 
		execute { Cassandra.Client c ->
			def sp = new SlicePredicate();
			SliceRange sr = new SliceRange();
			sr.setCount 100
			sr.setStart "".bytes
			sr.setFinish("".bytes)
			sp.setSlice_range sr
			def kr = new KeyRange();
			kr.start_key = "bar";
			kr.end_key = "foo";
			def res = c.get_range_slices(keyspace, new ColumnParent(cf), sp , kr , ConsistencyLevel.QUORUM)
						
			def m = [:]
			res.each {
				c.remove keyspace, it.key, new ColumnPath(cf), new Date().getTime(), ConsistencyLevel.QUORUM
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
