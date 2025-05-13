package com.nosql;
import java.util.ArrayList;
import java.util.Scanner;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
class Executor{
    public Hive hive;
    public PostgreSQL postgre;
    public MongoDB mongo;
    public boolean isexited = false;
    public Executor() throws Exception{
        this.hive = new Hive();
        this.mongo = new MongoDB();
        this.postgre = new PostgreSQL();
    }
    public void execStmt(String line) throws Exception{
        if(line.trim().toLowerCase().equals("exit")){
            hive.merge("postgresql");
            hive.merge("mongo");
            mongo.merge("postgresql");
            mongo.merge("hive");
            postgre.merge("mongo");
            postgre.merge("hive");
            
            return;
        }
        String[] command = line.split("\\.");
        String systemToExecuteOn = command[0].trim().toLowerCase();
        server sys_to_exec;
        if(systemToExecuteOn.equals("postgresql")){
            sys_to_exec = this.postgre;
        }else if(systemToExecuteOn.equals("mongo")){
            sys_to_exec = this.mongo;
        }else if(systemToExecuteOn.equals("hive")){
            sys_to_exec = this.hive;
        }else{
            throw new Exception("Unknown System must be one of (postgresql/mongo/hive)");
        }
        String rest_of_cmd = command[1];
        int open_bracket_idx = rest_of_cmd.indexOf("(");
        String commandx = rest_of_cmd.substring(0,open_bracket_idx).trim().toLowerCase(),args = rest_of_cmd.substring(open_bracket_idx+1,rest_of_cmd.length()-1);
        if(commandx.equals("set")){
            String[] argv = args.split(",");
            String sid = argv[0].trim();
            String cid = argv[1].trim();
            String grade = argv[2].trim();
            sys_to_exec.set(sid,cid,grade);
        }else if(commandx.equals("get")){
            String[] argv = args.split(",");
            String sid = argv[0].trim();
            String cid = argv[1].trim();
            ArrayList<String> result = sys_to_exec.get(sid, cid);
            System.out.println("rollno : " + result.get(0) + "\nemailid : " + result.get(1) + "\ngrade : "+result.get(2));
        }else if(commandx.equals("merge")){
            String sysToMerge = args.trim().toLowerCase();
            sys_to_exec.merge(sysToMerge);
        }else{
            throw new Exception("Unknown Command must be one of (set/get/merge)");
        }
    }
    public void Interpret() throws Exception{
        Scanner stdin = new Scanner(System.in);
        while(true){
            System.out.print(">>> ");
            String inp = stdin.nextLine().trim();
            this.execStmt(inp);
            if(inp.trim().toLowerCase().equals("exit")){
                this.isexited = true;
                break;
            }
        }
        stdin.close();
    }
    public void testCases(String path) throws Exception{
        BufferedReader fd = new BufferedReader(new FileReader(path));
        String line;
        while((line = fd.readLine()) != null){
            String inp = line.trim();
            System.out.println("\n>>> " + inp + "\n");
            this.execStmt(inp);
            if(inp.trim().toLowerCase().equals("exit")){
                this.isexited = true;
                break;
            }
        }
        fd.close();
    }
    public void close() throws Exception{
        this.hive.close();
        this.mongo.close();
        this.postgre.close();
        if(this.isexited){
            File file1 = new File("hive.log");
            file1.delete();
            File file2 = new File("mongo.log");
            file2.delete();
            File file3 = new File("postgresql.log");
            file3.delete();
        }
    }
}
public class Main {
    public static void main(String[] args) {
        try{
            Executor exec = new Executor();
            if(args.length == 0){
                exec.Interpret();
            }else{
                exec.testCases(args[0]);
            }
            exec.close();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }
}