package com.nosql;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.File;
import java.util.ArrayList;
import com.nosql.Pair;
import com.nosql.logobj;
public class logger {
    BufferedWriter writer;
    String mylogfile;
    public logger(String mylog) throws Exception
    {
        this.mylogfile=mylog;
        this.writer = new BufferedWriter(new FileWriter(mylog, true));

    }
    public void write(logobj obj) throws IOException {
        // Format the log line
        if(obj.grade!=null)
        {
            String line = String.format("%s,%s,%s,%s,%s\n", obj.op, obj.sid, obj.cid, obj.grade, obj.ts.toString());
            writer.write(line);
        }
        else
        {
            String line = String.format("%s,%s,%s,%s\n", obj.op, obj.sid, obj.cid, obj.ts.toString());
            writer.write(line);
        }
        writer.flush(); // ensure it writes to disk
    }
    public Pair<ArrayList<logobj>,Integer> read(String otherlog,int offset) throws IOException
    {
        int count = 1;
        BufferedReader reader = new BufferedReader(new FileReader(otherlog));
        String line;
        while(count<=offset && (line = reader.readLine()) != null)
        {
            count++;
        }
        ArrayList<logobj> arr = new ArrayList<>();
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split(",");
            if(parts[0].trim().equals("set"))
            {
                logobj obj = new logobj(parts[0], parts[1], parts[2], parts[3], java.sql.Timestamp.valueOf(parts[4]));
                arr.add(obj);
            }
            offset++;
        }
        reader.close();
        return new Pair<>(arr,offset);
    }
    public void close() throws Exception {
        writer.close();
        // File file = new File(this.mylogfile);
        // file.delete();
    }
}
