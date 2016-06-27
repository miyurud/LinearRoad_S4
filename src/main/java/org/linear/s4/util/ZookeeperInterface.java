package org.linear.s4.util;

import java.util.logging.Logger;

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;

import org.linear.s4.util.hadoopbook.CreateGroup;
import org.linear.s4.util.hadoopbook.ActiveKeyValueStore;

public class ZookeeperInterface {
	static Logger logger = Logger.getLogger(ZookeeperInterface.class.getName());
//	public static String getValueAtPath(String path){
//		String result = null;
//		
//		Zookeeper zk = new Zookeeper("127.0.0.1", 2000, this); // Session timeout is in milliseconds. We assume that there is already a Zookeeper instance running in the local host.
//		boolean fl = zk.exists(path, false);
//		
//		//"/lr/history/host"
//		if(fl){
//			
//		}
//		
//		return result;
//	}
	
	public static void setValueAtPath(String path, String value){
		try{
			ActiveKeyValueStore store = new ActiveKeyValueStore();
			store.connect("127.0.0.1");
		
			store.write(path, value);
			store.close();
		}catch(Exception e){
			logger.info(e.getMessage());
		}
	}
	
	public static String getValueAtPath(String path){
		String result = null;
		try{
			ActiveKeyValueStore store = new ActiveKeyValueStore();
			store.connect("127.0.0.1");
		
			result = store.read(path, null);
			store.close();
		}catch(Exception e){
			logger.info(e.getMessage());
		}
		
		return result;
	}
	
	public static boolean createGroup(String path){
	    try{
	    	CreateGroup createGroup = new CreateGroup();
	    	createGroup.connect("127.0.0.1");

	    	createGroup.create(path);
		    createGroup.close();
	    }catch(Exception ec){
	    	logger.info(ec.getMessage());
	    	return false;
	    }
	    
	    return true;
	}
	
}
