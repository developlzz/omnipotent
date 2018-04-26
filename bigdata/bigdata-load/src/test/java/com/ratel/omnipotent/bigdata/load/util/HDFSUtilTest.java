package com.ratel.omnipotent.bigdata.load.util;

public class HDFSUtilTest {
	public static boolean testCreateFolder(String root, String folderName) throws Exception {
		return HDFSUtil.mkdir(root, folderName);
	}
	
	public static void main(String[] args) {
		try {
			String root = "hdfs://10.10.21.71:9000";
			boolean b = false;
			
			System.setProperty("HADOOP_USER_NAME", "root");
//			b = testCreateFolder(root, "qiu_test");
//			b = HDFSUtil.deleteDir(root, "qiu_test");
			b = HDFSUtil.uploadLocalFile2HDFS(root, "E:\\maven-dep.txt", "maven.txt");
			System.out.println(b ? "成功" : "失败");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
