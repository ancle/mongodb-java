package com.sxzjrj.mongodb.java;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

public class MongoDBJavaTest {
	private MongoClient client = null;
	private MongoDatabase mongoDB = null;
	private MongoCollection<Document> doc = null;

	public void initWithCredential() {
		// 包装mongodb server地址为ServerAddress对象
		List<ServerAddress> servers = new ArrayList<ServerAddress>();
		ServerAddress server = new ServerAddress("127.0.0.1", 27027);
		servers.add(server);

		// 创建mongo 
		MongoCredential credential = MongoCredential.createScramSha1Credential("username", "",
				new String("").toCharArray());
		List<MongoCredential> credentials = new ArrayList<MongoCredential>();
		credentials.add(credential);
		
		client = new MongoClient(servers, credentials);
	}

	@Before
	public void init() {
		client = new MongoClient("127.0.0.1", 27027);
		mongoDB = client.getDatabase("yiwq");
		doc = mongoDB.getCollection("users");
	}

	@Test
	public void testInsert() {
		Document u1 = new Document();
		u1.append("username", "yiwq");
		u1.append("age", 43);
		u1.append("salary", 8898.5);
		u1.append("country", "china");
		Map<String, Object> address = new HashMap<String, Object>();
		address.put("code", "033001");
		address.put("addr", "山西运城");
		u1.append("address", address);
		Map<String, Object> favorites = new HashMap<String, Object>();
		favorites.put("movies", Arrays.asList("山的那边", "叶问", "湄公河惨案", "战狼"));
		favorites.put("cities", Arrays.asList("NewYork", "Beijing", "Shanghai"));
		u1.append("favorites", favorites);

		doc.insertOne(u1);
	}

	@Test
	public void testUpdate() {
		UpdateResult result = doc.updateOne(Filters.eq("username", "yiwq"),
				new Document("$set", new Document("username", "ancle")));
		System.out.println("update result = " + result.getUpsertedId());
	}
	
	@Test
	public void testFind() {
		FindIterable<Document> result = doc.find(Filters.in("country", Arrays.asList("china", "USA")));
		MongoCursor<Document>  iterator = result.iterator();
		while (iterator.hasNext()) {
			System.out.println(iterator.next().toString());
		}
	}
	
	@Test
	public void testDelete() {
		DeleteResult result = doc.deleteOne(Filters.eq("username", "ancle"));
		System.out.println("delete result = " + result.getDeletedCount());
	}
}
