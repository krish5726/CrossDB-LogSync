package com.nosql;
import com.nosql.server;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import java.sql.Timestamp;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;
import java.util.ArrayList;
import com.mongodb.client.FindIterable;
import java.io.BufferedReader;
import java.io.FileReader;
public class MongoDB implements server{
    private logger log;
    private int postgresoffset=0,hiveoffset=0;
    private MongoClient mongoClient;
    private MongoDatabase db;
    public MongoDB() throws Exception
    {
        this.mongoClient = MongoClients.create("mongodb://localhost:27017");
        this.db = mongoClient.getDatabase("testdb");
        this.log = new logger("mongo.log");
    }
    @Override
    public void set(String sid,String cid,String grade) throws Exception
    {
        MongoCollection<Document> collection = db.getCollection("student_course_grades");
        Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());

        // Perform update
        collection.updateOne(
            and(eq("studentid", sid), eq("courseid", cid)),
            combine(
                com.mongodb.client.model.Updates.set("grade", grade),
                com.mongodb.client.model.Updates.set("last_modified", currentTimestamp)
            )
        );

        System.out.println("Data updated successfully in MongoDB");

        // Log the operation
        logobj obj = new logobj("set", sid, cid, grade, currentTimestamp);
        log.write(obj);
    }
    @Override
    public ArrayList<String> get(String sid,String cid) throws Exception
    {
        MongoCollection<Document> collection = db.getCollection("student_course_grades");
        FindIterable<Document> rs = collection.find(and(eq("studentid",sid),eq("courseid",cid)));
        ArrayList<String> arr = new ArrayList<>();
        for (Document doc : rs) {
            arr.add(doc.getString("rollno"));
            arr.add(doc.getString("emailid"));
            arr.add(doc.getString("grade"));
        }
        logobj obj = new logobj("get", sid, cid, null, new java.sql.Timestamp(System.currentTimeMillis()));
        log.write(obj);
        return arr;
    }
    public void merge(String system) throws Exception
    {
        String filename = system+".log";
        int offset_to_pass;
        if(system.equals("postgresql"))
        {
            offset_to_pass=postgresoffset;
        }
        else if(system.equals("mongo"))
        {
            return;
        }
        else if(system.equals("hive")){
            offset_to_pass=hiveoffset;
        }
        else 
        {
            throw new Exception("wrong system provided for merge");
        }
        Pair<ArrayList<logobj>,Integer> result = log.read(filename,offset_to_pass);
        if(system.equals("postgresql"))
        {
            postgresoffset=result.second;
        } 
        else
        {
            hiveoffset=result.second;
        }
        for(logobj temp:result.first)
        {
            String sid=temp.sid,cid=temp.cid;
            Timestamp ts = temp.ts;
            String grade = temp.grade;
            MongoCollection<Document> collection = db.getCollection("student_course_grades");
            FindIterable<Document> rs = collection.find(and(eq("studentid",sid),eq("courseid",cid)));
            Document doc = rs.first();
            if(doc!=null && doc.getDate("last_modified").before(ts))
            {
                collection.updateOne(
                    and(eq("studentid", sid), eq("courseid", cid)),
                    combine(
                        com.mongodb.client.model.Updates.set("grade", grade),
                        com.mongodb.client.model.Updates.set("last_modified", ts)
                    )
                );
                logobj obj = new logobj("set", sid, cid, grade, ts);
                this.log.write(obj);
            }
        }
    }
    @Override
    public void close() throws Exception{
        this.log.close();
        this.mongoClient.close();
    }
}
