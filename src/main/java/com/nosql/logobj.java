package com.nosql;

import java.sql.Timestamp;

public class logobj {
    public String op,sid,cid,grade;
    public Timestamp ts;
    public logobj(String op,String sid,String cid,String grade,Timestamp ts){
        this.op=op;
        this.sid=sid;
        this.cid=cid;
        this.grade=grade;
        this.ts=ts;
    }
}
