package com.son.riakycsb;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Vector;

import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

//TODO: response.close should be in finally{} block so that an exception
//wouldn't prevent response from closing

public class RiakClient extends DB
{
    public static Set<Integer> OK_FETCH_CODES = 
            new HashSet<Integer>(Arrays.asList(200, 300, 304));
    
    public static Set<Integer> OK_STORE_CODES = 
            new HashSet<Integer>(Arrays.asList(200, 201, 204, 300));
    
    public static Set<Integer> OK_DELETE_CODES = 
            new HashSet<Integer>(Arrays.asList(204, 404));
    
    public static int UNDEFINED_ERROR_CODE = -1;
    
    CloseableHttpClient httpClient = HttpClients.createDefault();
    Random random = new Random();
    
    String[] myhosts;
    int readDelay;
  public void init() throws DBException
  {
    String hosts = getProperties().getProperty("hosts");
    if (hosts == null)
    {
      throw new DBException(
              "Required property \"hosts\" missing for RiakClient");
    }
    
    myhosts = hosts.split(",");
    
    String delay = getProperties().getProperty("readDelay");
    readDelay = delay == null? 0 : Integer.parseInt(delay);
  }
  
  //Read a single record
  public int read(String table, String key, Set<String> fields, 
      HashMap<String,ByteIterator> result)
  {
    int statusCode = 0;
    String content;
     
    String url = makeURLRead(table, key);
    HttpGet httpGet = new HttpGet(url);
 
    try
    {
        CloseableHttpResponse response = httpClient.execute(httpGet);
        statusCode = response.getStatusLine().getStatusCode();
        HttpEntity entity = response.getEntity();
        content = EntityUtils.toString(entity);
        EntityUtils.consume(entity);
        response.close();
    } 
    catch (ClientProtocolException e)
    {
        e.printStackTrace();
        return UNDEFINED_ERROR_CODE;
    } 
    catch (IOException e)
    {
        e.printStackTrace();
        return UNDEFINED_ERROR_CODE;
    }
    
    if (!OK_FETCH_CODES.contains(statusCode))
    {
        return statusCode;
    }
    
    /*Pattern pattern = Pattern.compile("(field\\d)=====(.*?)#####");
    Matcher matcher = pattern.matcher(content);
    while (matcher.find()) {
    	int count = matcher.groupCount();
      for (int i = 1; i <= count; i++)
      {
      	result.put(matcher.group(1), new StringByteIterator(matcher.group(2)));
      }
    }*/
    
    result.put("Data", new StringByteIterator(content));
    
    return 0;
  }

  //Perform a range scan
  public int scan(String table, String startkey, int recordcount, 
      Set<String> fields, Vector<HashMap<String,ByteIterator>> result)
  {
    System.out.println("ERROR: scan is not implemented"); 
    return 0;
  }
    
  //Update a single record
  public int update(String table, String key, 
      HashMap<String,ByteIterator> values)
  {
    return insert(table, key, values);
  }

  //Insert a single record
  public int insert(String table, String key, 
      HashMap<String,ByteIterator> values)
  {
  	//TODO: change format to JSON
    String url = makeURLWrite(table, key);
    HttpPost httpPost = new HttpPost(url);
    
    StringBuilder builder = new StringBuilder();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      //String field = entry.getKey();
      String value = entry.getValue().toString();
      builder.append(value);
    }
    
    String content = builder.toString();
    HttpEntity entity;
    int statusCode = 0;
    
    try
    {
        entity = new StringEntity(content, 
                ContentType.create("text/plain", "UTF-8"));
        httpPost.setEntity(entity);
        CloseableHttpResponse response = httpClient.execute(httpPost);
        statusCode = response.getStatusLine().getStatusCode();
        response.close();
    } 
    catch (UnsupportedEncodingException e)
    {
        e.printStackTrace();
        return UNDEFINED_ERROR_CODE;
    } 
    catch (ClientProtocolException e)
    {
        e.printStackTrace();
        return UNDEFINED_ERROR_CODE;
    } 
    catch (IOException e)
    {
        e.printStackTrace();
        return UNDEFINED_ERROR_CODE;
    }
    
    if (!OK_STORE_CODES.contains(statusCode))
    {
        return statusCode;
    }
    
    return 0;
  }

  //Delete a single record
  public int delete(String table, String key)
  {
    String url = makeURLWrite(table, key);
    HttpDelete httpDelete = new HttpDelete(url);
    
    int statusCode = 0;
    
    try
    {
        CloseableHttpResponse response = httpClient.execute(httpDelete);
        statusCode = response.getStatusLine().getStatusCode();
        response.close();
    } 
    catch (UnsupportedEncodingException e)
    {
        e.printStackTrace();
        return UNDEFINED_ERROR_CODE;
    } 
    catch (ClientProtocolException e)
    {
        e.printStackTrace();
        return UNDEFINED_ERROR_CODE;
    } 
    catch (IOException e)
    {
        e.printStackTrace();
        return UNDEFINED_ERROR_CODE;
    }
    
    if (!OK_DELETE_CODES.contains(statusCode))
    {
        return statusCode;
    }
    
    return 0;
  }
  
  private String randomHost()
  {
  	return myhosts[random.nextInt(myhosts.length)];
  }
  private String makeURLRead(String table, String key)
  {
    return String.format("http://%s/buckets/%s/keys/%s?r=1&read_delay=%d", randomHost(), table, key, readDelay);
  }
  
  private String makeURLWrite(String table, String key)
  {
    return String.format("http://%s/buckets/%s/keys/%s?returnbody=false", randomHost(), table, key);
  }
  
}