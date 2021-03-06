package com.ratel.omnipotent.bigdata.load.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HDFSUtil {
	public static final String DEFAULT_SPLIT = "/";
	public static boolean mkdir(String root, String dir) throws IOException {  
        if (StringUtils.isBlank(dir)) {  
            return false;  
        }  
        String fullPath = getFullPath(root, dir);
        FileSystem fs = getFileSystem(root);
        
        if (!fs.exists(new Path(fullPath))) {  
            fs.mkdirs(new Path(fullPath));  
        }  
  
        fs.close();  
        return true;  
    }  
	
	public static boolean deleteDir(String root, String dir) throws IOException {  
        if (StringUtils.isBlank(dir)) {  
            return false;  
        }  
        String fullPath = getFullPath(root, dir);
        FileSystem fs = getFileSystem(root);
        fs.delete(new Path(fullPath), true);  
        fs.close();  
        return true;  
    }  
	
	public static List<String> listAll(String root, String dir) throws IOException {  
        if (StringUtils.isBlank(dir)) {  
            return new ArrayList<String>();  
        }  
        String fullPath = getFullPath(root, dir);
        FileSystem fs = getFileSystem(root);
        FileStatus[] stats = fs.listStatus(new Path(fullPath));  
        List<String> names = new ArrayList<String>();  
        for (int i = 0; i < stats.length; ++i) {  
            if (stats[i].isFile()) {  
                // regular file  
                names.add(stats[i].getPath().toString());  
            } else if (stats[i].isDirectory()) {  
                // dir  
                names.add(stats[i].getPath().toString());  
            } else if (stats[i].isSymlink()) {  
                // is s symlink in linux  
                names.add(stats[i].getPath().toString());  
            }  
        }  
  
        fs.close();  
        return names;  
    }  
	
	public static boolean uploadLocalFile2HDFS(String root, String localFile, String hdfsFile) throws IOException {  
        if (StringUtils.isBlank(localFile) || StringUtils.isBlank(hdfsFile)) {  
            return false;  
        }  
        String fullPath = getFullPath(root, hdfsFile);
        FileSystem fs = getFileSystem(root);
        Path src = new Path(localFile);  
        Path dst = new Path(fullPath);  
        fs.copyFromLocalFile(src, dst);  
        fs.close();  
        return true;  
    }  
	
	public static boolean createNewHDFSFile(String root, String newFile, String content) throws IOException {  
        if (StringUtils.isBlank(newFile) || null == content) {  
            return false;  
        }  
        String fullPath = getFullPath(root, newFile);
        FileSystem fs = getFileSystem(root);
        FSDataOutputStream os = fs.create(new Path(fullPath));  
        os.write(content.getBytes("UTF-8"));  
        os.close();  
        fs.close();  
        return true;  
    }  
	
	public static boolean deleteHDFSFile(String root, String hdfsFile) throws IOException {  
        if (StringUtils.isBlank(hdfsFile)) {  
            return false;  
        }  
        String fullPath = getFullPath(root, hdfsFile);
        FileSystem fs = getFileSystem(root);
        Path path = new Path(fullPath);  
        boolean isDeleted = fs.delete(path, true);  
        fs.close();  
        return isDeleted;  
    }  
	
	public static byte[] readHDFSFile(String root, String hdfsFile) throws Exception {  
        if (StringUtils.isBlank(hdfsFile)) {  
            return null;  
        }  
        String fullPath = getFullPath(root, hdfsFile);
        FileSystem fs = getFileSystem(root);
        // check if the file exists  
        Path path = new Path(fullPath);  
        if (fs.exists(path)) {  
            FSDataInputStream is = fs.open(path);  
            // get the file info to create the buffer  
            FileStatus stat = fs.getFileStatus(path);  
            // create the buffer  
            byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))];  
            is.readFully(0, buffer);  
            is.close();  
            fs.close();  
            return buffer;  
        } else {  
            throw new Exception("the file is not found .");  
        }  
    }  
	
	public static boolean append(String root, String hdfsFile, String content) throws Exception {  
        if (StringUtils.isBlank(hdfsFile)) {  
            return false;  
        }  
        if(StringUtils.isEmpty(content)){  
            return true;  
        }  
  
        String fullPath = getFullPath(root, hdfsFile);
        Configuration conf = getConfiguration();   
        // solve the problem when appending at single datanode hadoop env    
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");  
        conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");  
        FileSystem fs = FileSystem.get(URI.create(root), conf);  
        // check if the file exists  
        Path path = new Path(fullPath);  
        if (fs.exists(path)) {  
            try {  
                InputStream in = new ByteArrayInputStream(content.getBytes());  
                OutputStream out = fs.append(new Path(fullPath));  
                IOUtils.copyBytes(in, out, 4096, true);  
                out.close();  
                in.close();  
                fs.close();  
            } catch (Exception ex) {  
                fs.close();  
                throw ex;  
            }  
        } else {  
        	HDFSUtil.createNewHDFSFile(root, hdfsFile, content);  
        }  
        return true;  
    }  
	
	public static String getFullPath(String root, String path) {
		String split = DEFAULT_SPLIT;
		String tmp = "";
		if (!tmp.endsWith(split)) {
			tmp = tmp.concat(split);
		}
		tmp = tmp.concat("test").concat(split);
		if (!path.startsWith(split)) {
			tmp = tmp.concat(path);
		} else {
			int idx = path.length() - split.length();
			tmp = tmp.concat(path.substring(0, idx));
		}
		return tmp;
	}
	
	public static Configuration getConfiguration() {
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());    
		return conf;
	}
	
	public static FileSystem getFileSystem(String root, Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(URI.create(root), conf);  
		return fs;
	}
	
	public static FileSystem getFileSystem(String root) throws IOException {
		return getFileSystem(root, getConfiguration());
	}
  
}
