package com.nosql;
import java.util.ArrayList;

public interface server {
    public void set(String sid,String cid,String grade) throws Exception;
    public ArrayList<String> get(String sid,String cid) throws Exception;
    public void merge(String system) throws Exception;
    public void close() throws Exception;
}
