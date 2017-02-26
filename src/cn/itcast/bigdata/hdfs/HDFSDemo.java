package cn.itcast.bigdata.hdfs;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

public class HDFSDemo {
	// 上传文件方式1:
	@Test
	public void testUpload() throws Exception {
		// 配置信息对象
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://mini1:9000");
		// 拿到一个文件系统操作的客户端实例对象,是一个抽象类
		FileSystem fs = FileSystem.get(new URI("hdfs://mini1:9000"), conf, "hadoop");
		// Thread.sleep(2000);
		// 将本地文件拷贝到fs文件系统的目录
		fs.copyFromLocalFile(new Path("D:/my.log"), new Path("/my.log.copy"));
		fs.close();
	}

	// 上传文件方式2:
	@Test
	public void testUpload_02() throws Exception {
		// 配置信息对象
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://mini1:9000");
		// 拿到一个文件系统操作的客户端实例对象,是一个抽象类
		FileSystem fs = FileSystem.get(new URI("hdfs://mini1:9000"), conf, "hadoop");
		InputStream in = new FileInputStream("d:/my.log");
		// 目标文件
		OutputStream out = fs.create(new Path("/my.log.copy02"));
		IOUtils.copyBytes(in, out, 4096, true);
	}

	// 通过javaAPI的原始方式下载文件
	@Test
	public void testDownload_01() throws Exception {
		// 连接好hdfs的老大master,最后一个参数为用户名
		FileSystem fs = FileSystem.get(new URI("hdfs://mini1:9000"), new Configuration(), "hadoop");
		// 源文件是HDFS文件系统的跟目录
		InputStream in = fs.open(new Path("/my.log.copy"));
		// 目标文件 ，win7系统下的D盘,下载并修改文件名称为my.log.toLocal
		OutputStream out = new FileOutputStream("d:/my.log.toLocal");
		IOUtils.copyBytes(in, out, 4096, true);
	}

	// 通过Hadoop的API下载文件，会报错，失败时也会在下载的目录也会生成文件，只是大小为0KB
	// 会多一个.my.log.copy.crc文件
	@Test
	public void testDownload_02() throws Exception {
		// 配置信息对象
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://mini1:9000");
		// 拿到一个文件系统操作的客户端实例对象,是一个抽象类
		FileSystem fs = FileSystem.get(new URI("hdfs://mini1:9000"), conf, "hadoop");
		// 将fs文件系统的文件下载到本地
		fs.copyToLocalFile(new Path("/my.log.copy"), new Path("D:/"));
		fs.close();
	}
	
	/**
	 * 创建目录
	 */
	@Test
	public void makdirTest() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://mini1:9000");
		FileSystem fs = FileSystem.get(new URI("hdfs://mini1:9000"), conf, "hadoop");

		boolean mkdirs = fs.mkdirs(new Path("/aaa"));
		System.out.println(mkdirs);
	}

	/**
	 * 删除文件或文件夹
	 */
	@Test
	public void testDelete() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://mini1:9000");
		FileSystem fs = FileSystem.get(new URI("hdfs://mini1:9000"), conf, "hadoop");

		boolean delete = fs.delete(new Path("/aaa"), true);// true:递归删除
		System.out.println(delete);
	}
}
