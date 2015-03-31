package com.serendio.mllib;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by praveen on 31/3/15.
 */
public class DBlookup
{
    static Connection m_con = null;
    static Statement m_stmt = null;
    public DBlookup()
    {

    }
    void establishDBConnection()
    {
        try
        {
            Class.forName("org.postgresql.Driver");
            m_con = DriverManager.getConnection("jdbc:postgresql://" + "178.63.22.132" + "/" + "vipin_test", "amc_engineer", "serendio123");
            m_stmt = m_con.createStatement();
        }
        catch (ClassNotFoundException ex)
        {
            ex.printStackTrace();
        }
        catch (SQLException ex)
        {
            ex.printStackTrace();
        }
    }
    static void closeConnection()
    {
        try
        {
            m_stmt.close();
            m_con.close();
        }
        catch(SQLException ex)
        {
            ex.printStackTrace();
        }
    }
    public Map<String,String> getDocumentsForCase(String caseno)
    {
        Map<String,String> docMap = new HashMap<String,String>();
        establishDBConnection();
        try
        {
            Statement stat = m_con.createStatement();
            String query = "select id,path from docs where docket_report_id in (select id from docket_report where case_id="+ caseno +") and source='User'";
            ResultSet res = m_stmt.executeQuery(query);
            String basePath = "/home/serendio/myproject/legalitee/tmp/";
            while(res.next())
            {
                String query2 = "select id from jobs where docs_id ="+res.getString("id");
                ResultSet res1 = stat.executeQuery(query2);

                String jobid="";
                if(res1.next())
                {
                    jobid = res1.getString("id");
                    String temp = res.getString("path");
                    temp = temp.substring(temp.lastIndexOf("/")+1, temp.length());
                    docMap.put(temp,jobid);
                }
            }
            closeConnection();
        }
        catch (SQLException ex)
        {
            ex.printStackTrace();
        }
        return getFilecontents(docMap);
    }
    Map<String,String> getFilecontents(Map<String,String> fileMap)
    {
        Map<String,String> fileOutMap = new HashMap<String,String>();
        try
        {
            establishDBConnection();
            for(String temp : fileMap.keySet())
            {
                String query = "select trimmed_content from docs_trimmed_contents where job_id="+fileMap.get(temp);
                ResultSet res = m_stmt.executeQuery(query);
                if(res.next())
                {
                    fileOutMap.put(temp,res.getString("trimmed_content"));
                }
            }
            closeConnection();
        }
        catch(SQLException e)
        {
            e.printStackTrace();
        }
        return fileOutMap;
    }
}
