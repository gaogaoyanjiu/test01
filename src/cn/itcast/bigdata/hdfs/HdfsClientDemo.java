package cn.itcast.bigdata.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

/**
 * 客户端去操作hdfs时，是有一个用户身份的
 * 默认情况下，hdfs客户端api会从jvm中获取一个参数来作为自己的用户身份：-DHADOOP_USER_NAME=hadoop
 * 
 * 也可以在构造客户端fs对象时，通过参数传递进去
 * 
 * @author
 */
public class HdfsClientDemo {
	FileSystem fs = null;
	Configuration conf = null;

	// 初始化
	@Before
	public void init() throws Exception {
		conf = new Configuration();// 抽象类
		conf.set("fs.defaultFS", "hdfs://mini1:9000");

		// 拿到一个文件系统操作的客户端实例对象
		// fs = FileSystem.get(conf);//默认是本地文件系统
		// 参数1:uri(指定的HDFS文件系统的老大),参数2:系统实例,参数3:伪造的用户身份
		fs = FileSystem.get(new URI("hdfs://mini1:9000"), conf, "hadoop");
	}

	// 上传文件
	@Test
	public void testUpload() throws Exception {

		Thread.sleep(2000);
		fs.copyFromLocalFile(new Path("D:/my.log"), new Path("/my.log.copy"));
		fs.close();
	}

	// 下载文件
	@Test
	public void testDownload() throws Exception {

		fs.copyToLocalFile(new Path("/my.log.copy"), new Path("D:/"));
		fs.close();
	}

	@Test
	public void testConf() {
		Iterator<Entry<String, String>> iterator = conf.iterator();
		while (iterator.hasNext()) {
			Entry<String, String> entry = iterator.next();
			System.out.println(entry.getValue() + "--" + entry.getValue());// conf加载的内容
		}
	}

	/**
	 * 创建目录
	 */
	@Test
	public void makdirTest() throws Exception {
		boolean mkdirs = fs.mkdirs(new Path("/aaa/bbb"));
		System.out.println(mkdirs);
	}

	/**
	 * 删除
	 */
	@Test
	public void deleteTest() throws Exception {
		boolean delete = fs.delete(new Path("/"), true);// true， 递归删除
		System.out.println(delete);
	}

	/**
	 * 查看目录信息，只显示文件
	 * 
	 * @throws IOException
	 * @throws IllegalArgumentException
	 * @throws FileNotFoundException
	 */
	@Test
	public void testListFiles() throws FileNotFoundException, IllegalArgumentException, IOException {

		// 思考：为什么返回迭代器，而不是List之类的容器
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

		while (listFiles.hasNext()) {
			LocatedFileStatus fileStatus = listFiles.next();
			System.out.println(fileStatus.getPath().getName());
			System.out.println(fileStatus.getBlockSize());
			System.out.println("权限:"+fileStatus.getPermission());
			System.out.println(fileStatus.getLen());
			BlockLocation[] blockLocations = fileStatus.getBlockLocations();
			for (BlockLocation bl : blockLocations) {
				System.out.println("块的长度:" + bl.getLength());
				System.out.println("块的偏移量:" + bl.getOffset());
				String[] hosts = bl.getHosts();
				for (String host : hosts) {
					System.out.println("主机名:"+host);
				}
			}
			System.out.println("--------------为angelababy打印的分割线--------------");
		}
	}

	/**
	 * 查看文件及文件夹信息
	 * 
	 * @throws IOException
	 * @throws IllegalArgumentException
	 * @throws FileNotFoundException
	 */
	@Test
	public void testListAll() throws FileNotFoundException, IllegalArgumentException, IOException {

		FileStatus[] listStatus = fs.listStatus(new Path("/"));

		String flag = "dir-----------------";
		for (FileStatus fstatus : listStatus) {
			//如果是文件就输出f------------
			if (fstatus.isFile())
				flag = "file-----------------";
			System.out.println(flag + fstatus.getPath().getName());
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://mini1:9000");
		// 拿到一个文件系统操作的客户端实例对象
		FileSystem fs = FileSystem.get(conf);

		fs.copyFromLocalFile(new Path("D:/my.log"), new Path("/my.log.copy"));
		fs.close();
	}
}
