package hector
/*package xebia.cassandraclient

import java.util.Date;

import me.prettyprint.cassandra.service.CassandraHostConfigurator 
import me.prettyprint.cassandra.service.Cluster 
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.cassandra.thrift.ConsistencyLevel;


class HectorWrapper {
	
	CassandraHostConfigurator chc = new CassandraHostConfigurator("127.0.0.1")
	Cluster cluster = getOrCreateCluster("Chat CLuster", chc)
	
	def getKeyspace(keyspace) {
		return new Keyspace(ks:keyspace,client:this);
	}
	
	def put(keyspace,cf,cn,key,value) {
		me.prettyprint.cassandra.service.Keyspace ks = createKeyspaceOperator(keyspace, cluster)
		ColumnPath cp = new ColumnPath(cf);
		cp.setColumn(cn.getBytes())
		ks.insert key,cp , value.getBytes()
	}
	
	def get(keyspace,cf,cn, key) {
		me.prettyprint.cassandra.service.Keyspace ks = createKeyspaceOperator(keyspace, cluster)
		ColumnPath cp = new ColumnPath(cf);
		cp.setColumn(cn.getBytes())
		ks.get key, cp
	}
	
	def count(keyspace,cf,cn, key) {
		me.prettyprint.cassandra.service.Keyspace ks = createKeyspaceOperator(keyspace, cluster)
		ColumnParent cp = new ColumnParent(cf);
		SlicePredicate sp = new SlicePredicate()
		ks.getCount key.bytes, cp, sp
	}
	
	def getSlice(keyspace,cf,key,cn) {
		me.prettyprint.cassandra.service.Keyspace ks = createKeyspaceOperator(keyspace, cluster)
		ColumnParent cp = new ColumnParent(cf);
		SlicePredicate sp = new SlicePredicate();
		//sp.column_names = [cn.getBytes()];
		SliceRange sr = new SliceRange();
		sr.setCount 100
		sr.setStart "".bytes
		sr.setFinish("".bytes)
		sp.setSlice_range sr
		ks.getSlice key, cp, sp
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
*/