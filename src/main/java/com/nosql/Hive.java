package com.nosql;

import java.sql.*;
import java.util.ArrayList;
import org.apache.hive.jdbc.HiveDriver;

import com.nosql.server;

public class Hive implements server {
    int mongooffset = 0, postgresoffset = 0;
    private String url = "jdbc:hive2://localhost:10000/default";
    String username = "";
    String password = "";
    private Connection conn;
    private logger log;

    public Hive() throws Exception {
        this.log = new logger("hive.log");
        DriverManager.registerDriver(new org.apache.hive.jdbc.HiveDriver());
        this.conn = DriverManager.getConnection(url, username, password);
        CreateTable();
    }
    
    private void CreateTable() throws Exception {
        Statement stmt1 = conn.createStatement();
        stmt1.execute("SET hive.exec.dynamic.partition = true");
        stmt1.execute("SET hive.exec.dynamic.partition.mode = nonstrict");
        stmt1.execute("SET hive.exec.max.dynamic.partitions = 10000");
        stmt1.execute("SET hive.exec.max.dynamic.partitions.pernode = 1000");
        stmt1.execute("SET hive.support.concurrency = true");
        stmt1.execute("SET hive.enforce.bucketing = true");
        stmt1.execute("SET hive.exec.dynamic.partition.mode = nonstrict");
        stmt1.execute("SET hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
        stmt1.execute("SET hive.compactor.initiator.on = true");
        stmt1.execute("SET hive.compactor.worker.threads = 1");
        // Check if the table already exists
        ResultSet tables = stmt1.executeQuery("SHOW TABLES LIKE 'student_course_grades'");
        if (tables.next()) {
            System.out.println("The tables already exists");
        } else {
            Statement stmt = conn.createStatement();

            stmt.execute("SET hive.exec.dynamic.partition = true");
            stmt.execute("SET hive.exec.dynamic.partition.mode = nonstrict");
            stmt.execute("SET hive.exec.max.dynamic.partitions = 10000");
            stmt.execute("SET hive.exec.max.dynamic.partitions.pernode = 1000");
            stmt.execute("SET hive.support.concurrency = true");
            stmt.execute("SET hive.enforce.bucketing = true");
            stmt.execute("SET hive.exec.dynamic.partition.mode = nonstrict");
            stmt.execute("SET hive.txn.manager = org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
            stmt.execute("SET hive.compactor.initiator.on = true");
            stmt.execute("SET hive.compactor.worker.threads = 1");

            String createStagingTableQuery = "CREATE TABLE student_course_grades_staged (" +
                    "studentid STRING, " +
                    "courseid STRING, " +
                    "rollno STRING, " +
                    "emailid STRING, " +
                    "grade STRING) " +
                    "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' " +
                    "WITH SERDEPROPERTIES (\"separatorChar\" = \",\") " +
                    "STORED AS TEXTFILE " +
                    "TBLPROPERTIES (\"skip.header.line.count\"=\"1\")";
            stmt.execute(createStagingTableQuery);
            System.out.println("Table student_course_grades_staged created.");
            String createTableQuery = "CREATE TABLE student_course_grades (" +
                    "studentid STRING, " +
                    "courseid STRING, " +
                    "rollno STRING, " +
                    "emailid STRING, " +
                    "grade STRING, " +
                    "last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP) " +
                    "STORED AS ORC " +
                    "TBLPROPERTIES ('transactional'='true')";

            stmt.execute(createTableQuery);
            System.out.println("Table student_course_grades created.");
            add_data();
        }
    }

    private void add_data() throws Exception {
        Statement stmt = conn.createStatement();

        // Load data from local CSV file into staging table
        stmt.execute(
                "LOAD DATA LOCAL INPATH '/opt/hive/student_course_grades.csv' INTO TABLE student_course_grades_staged");

        // Insert data from staging table into the main table
        stmt.execute(
                "INSERT INTO TABLE student_course_grades (studentid, courseid, rollno, emailid, grade) " +
                        "SELECT studentid, courseid, rollno, emailid, grade FROM student_course_grades_staged");

        System.out.println("Data loaded into student_course_grades from staged table.");
    }

    @Override
    public void set(String sid, String cid, String grade) throws Exception {
        PreparedStatement setstatement = conn.prepareStatement(
                "UPDATE student_course_grades SET grade = ?, last_modified = ? WHERE studentid = ? AND courseid = ?");
        Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());
        setstatement.setString(1, grade);
        setstatement.setTimestamp(2, currentTimestamp); // current date and time
        setstatement.setString(3, sid);
        setstatement.setString(4, cid);
        setstatement.executeUpdate(); // Don't forget to actually execute the query!
        System.out.println("Data updated successfully");
        logobj obj = new logobj("set", sid, cid, grade, currentTimestamp);
        log.write(obj);
    }

    @Override
    public ArrayList<String> get(String sid, String cid) throws Exception {
        PreparedStatement getstatement = conn.prepareStatement(
                "select grade,rollno,emailid from student_course_grades where studentid=? and courseid=?");
        getstatement.setString(1, sid);
        getstatement.setString(2, cid);
        ResultSet rs = getstatement.executeQuery();
        ArrayList<String> arr = new ArrayList<>();
        rs.next();
        arr.add(rs.getString("rollno"));
        arr.add(rs.getString("emailid"));
        arr.add(rs.getString("grade"));
        logobj obj = new logobj("get", sid, cid, null, new java.sql.Timestamp(System.currentTimeMillis()));
        log.write(obj);
        return arr;
    }

    @Override
    public void merge(String system) throws Exception {
        String filename = system + ".log";
        int offset_to_pass;
        if (system.equals("mongo")) {
            offset_to_pass = mongooffset;
        } else if (system.equals("postgresql")) {
            offset_to_pass = postgresoffset;
        } else if (system.equals("hive")) {
            return;
        } else {
            throw new Exception("wrong system provided for merge");
        }
        Pair<ArrayList<logobj>, Integer> result = log.read(filename, offset_to_pass);
        if (system.equals("mongo")) {
            mongooffset = result.second;
        } else {
            postgresoffset = result.second;
        }
        for (logobj temp : result.first) {
            String sid = temp.sid, cid = temp.cid;
            Timestamp ts = temp.ts;
            String grade = temp.grade;
            PreparedStatement getstatement = conn.prepareStatement(
                    "select last_modified from student_course_grades where studentid=? and courseid=?");
            getstatement.setString(1, sid);
            getstatement.setString(2, cid);
            ResultSet rs = getstatement.executeQuery();
            rs.next();
            if (rs.getTimestamp("last_modified").compareTo(ts) < 0) {
                PreparedStatement setstatement = conn.prepareStatement(
                        "UPDATE student_course_grades SET grade = ?, last_modified = ? WHERE studentid = ? AND courseid = ?");
                setstatement.setString(1, grade);
                setstatement.setTimestamp(2, ts); 
                setstatement.setString(3, sid);
                setstatement.setString(4, cid);
                setstatement.executeUpdate(); 
                logobj obj = new logobj("set", sid, cid, grade, ts);
                this.log.write(obj);
            }
        }
    }

    @Override
    public void close() throws Exception {
        this.log.close();
        this.conn.close();
    }
}
