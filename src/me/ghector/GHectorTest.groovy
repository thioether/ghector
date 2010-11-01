package me.ghector;

import groovy.util.GroovyTestCase;

class GHectorTest extends GroovyTestCase {
	
	GHector client;
	Keyspace k;
	
	void setUp() throws Exception {
		client = new GHector(server:"127.0.0.1", port:9160)
		k = new Keyspace(ks:"ChatStore", client:client)
		k.truncate();
	}
	
	void testSingleColumnGetAndSet() {
		k["User/a/user_id"] = "1";
		assert k["User/a/user_id"] == "1";
	}
	
	void testSlice() {
		k["User/b/1"] = "1";
		k["User/b/2"] = "2";
		k["User/b/3"] = "3";
		
		assert k["User/b/[1,2,3]"] == [b:["1", "2", "3"]];
		assert k["User/b/[1-2]"] == [b:["1", "2"]];
		assert k["User/b/[3]"] == [b:["3"]];
	}
		
	void testRangeSlices() {
		k["User/d/1"] = "1";
		k["User/d/2"] = "2";

		k["User/e/1"] = "1";
		k["User/e/2"] = "2";

		assert k["User/[e-e]/[1,2,3]"] == [e:["1","2"]];
		assert k["User/[d-e]/[1,2,3]"] == [d:["1","2"],e:["1","2"]];
	}
	
/*	void testCount() {
		k["User/a/user_id"] = "2";
		k["User/a/sha1_user_pass"] = "pass_hash";
		k["User/a/online_status"] = 1;
		k["User/a/failed_logins"] = 1;
		k["User/a/account_status"] = 0;

		//assert k.count("User/a/") == 5
	}*/

}