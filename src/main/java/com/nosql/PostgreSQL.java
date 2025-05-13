package com.nosql;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.*;
import org.postgresql.Driver;

public class PostgreSQL implements server{
    private int mongooffset=0,hiveoffset=0;
    private String url = "jdbc:postgresql://localhost:5432/testdb";
    private String user = "postgres";
    private String password = "1234";
    private Connection conn ;
    private logger log;
    public PostgreSQL() throws Exception
    {  
        this.log = new logger("postgresql.log");
        DriverManager.registerDriver(new org.postgresql.Driver());
        this.conn = DriverManager.getConnection(url, user, password);
    }
    @Override
    public void set(String sid, String cid, String grade) throws Exception 
    {
        PreparedStatement setstatement = conn.prepareStatement(
            "UPDATE student_course_grades SET grade = ?, last_modified = ? WHERE studentid = ? AND courseid = ?"
        );
        Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());
        setstatement.setString(1, grade);
        setstatement.setTimestamp(2, currentTimestamp); 
        setstatement.setString(3, sid);
        setstatement.setString(4, cid);
    
        setstatement.executeUpdate(); 
        System.out.println("Data updated successfully in POSTGRESQL");
        logobj obj = new logobj("set",sid,cid,grade,currentTimestamp);
        log.write(obj);
    }
    @Override 
    public ArrayList<String> get(String sid, String cid) throws Exception
    {
        PreparedStatement getstatement = conn.prepareStatement(
            "select grade,rollno,emailid from student_course_grades where studentid=? and courseid=?"
        );
        getstatement.setString(1, sid);
        getstatement.setString(2, cid);
        ResultSet rs = getstatement.executeQuery();
        ArrayList<String> arr = new ArrayList<>();
        rs.next();
        arr.add(rs.getString("rollno"));
        arr.add(rs.getString("emailid"));
        arr.add(rs.getString("grade"));
        logobj obj = new logobj("get",sid,cid,null,new java.sql.Timestamp(System.currentTimeMillis()));
        log.write(obj);
        return arr;
    }
    @Override
    public void merge(String system) throws Exception
    {
        String filename = system+".log";
        int offset_to_pass;
        if(system.equals("mongo"))
        {
            offset_to_pass=mongooffset;
        }
        else if(system.equals("hive")){
            offset_to_pass=hiveoffset;
        }
        else if(system.equals("postgresql"))
        {
            return;
        }
        else 
        {
            throw new Exception("wrong system provided for merge");
        }
        Pair<ArrayList<logobj>,Integer> result = log.read(filename,offset_to_pass);
        if(system.equals("mongo"))
        {
            mongooffset=result.second;
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
            PreparedStatement getstatement = conn.prepareStatement(
                "select last_modified from student_course_grades where studentid=? and courseid=?"
            );
            getstatement.setString(1, sid);
            getstatement.setString(2, cid);
            ResultSet rs = getstatement.executeQuery();
            rs.next();
            if(rs.getTimestamp("last_modified").compareTo(ts)<0)
            {
                PreparedStatement setstatement = conn.prepareStatement(
                    "UPDATE student_course_grades SET grade = ?, last_modified = ? WHERE studentid = ? AND courseid = ?"
                );
                setstatement.setString(1, grade);
                setstatement.setTimestamp(2,ts); 
                setstatement.setString(3, sid);
                setstatement.setString(4, cid);
                setstatement.executeUpdate(); 
                logobj obj = new logobj("set", sid, cid, grade, ts);// we need to add merge as a kind of set statement in log so that other system when merge with this is consistent.
                this.log.write(obj);
            }
        }
    }
    @Override
    public void close() throws Exception{
        this.log.close();
        this.conn.close();
    }
}
