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
	// �ϴ��ļ���ʽ1:
	@Test
	public void testUpload() throws Exception {
		// ������Ϣ����
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://mini1:9000");
		// �õ�һ���ļ�ϵͳ�����Ŀͻ���ʵ������,��һ��������
		FileSystem fs = FileSystem.get(new URI("hdfs://mini1:9000"), conf, "hadoop");
		// Thread.sleep(2000);
		// �������ļ�������fs�ļ�ϵͳ��Ŀ¼
		fs.copyFromLocalFile(new Path("D:/my.log"), new Path("/my.log.copy"));
		fs.close();
	}

	// �ϴ��ļ���ʽ2:
	@Test
	public void testUpload_02() throws Exception {
		// ������Ϣ����
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://mini1:9000");
		// �õ�һ���ļ�ϵͳ�����Ŀͻ���ʵ������,��һ��������
		FileSystem fs = FileSystem.get(new URI("hdfs://mini1:9000"), conf, "hadoop");
		InputStream in = new FileInputStream("d:/my.log");
		// Ŀ���ļ�
		OutputStream out = fs.create(new Path("/my.log.copy02"));
		IOUtils.copyBytes(in, out, 4096, true);
	}

	// ͨ��javaAPI��ԭʼ��ʽ�����ļ�
	@Test
	public void testDownload_01() throws Exception {
		// ���Ӻ�hdfs���ϴ�master,���һ������Ϊ�û���
		FileSystem fs = FileSystem.get(new URI("hdfs://mini1:9000"), new Configuration(), "hadoop");
		// Դ�ļ���HDFS�ļ�ϵͳ�ĸ�Ŀ¼
		InputStream in = fs.open(new Path("/my.log.copy"));
		// Ŀ���ļ� ��win7ϵͳ�µ�D��,���ز��޸��ļ�����Ϊmy.log.toLocal
		OutputStream out = new FileOutputStream("d:/my.log.toLocal");
		IOUtils.copyBytes(in, out, 4096, true);
	}

	// ͨ��Hadoop��API�����ļ����ᱨ����ʧ��ʱҲ�������ص�Ŀ¼Ҳ�������ļ���ֻ�Ǵ�СΪ0KB
	// ���һ��.my.log.copy.crc�ļ�
	@Test
	public void testDownload_02() throws Exception {
		// ������Ϣ����
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://mini1:9000");
		// �õ�һ���ļ�ϵͳ�����Ŀͻ���ʵ������,��һ��������
		FileSystem fs = FileSystem.get(new URI("hdfs://mini1:9000"), conf, "hadoop");
		// ��fs�ļ�ϵͳ���ļ����ص�����
		fs.copyToLocalFile(new Path("/my.log.copy"), new Path("D:/"));
		fs.close();
	}
	
	/**
	 * ����Ŀ¼
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
	 * ɾ���ļ����ļ���
	 */
	@Test
	public void testDelete() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://mini1:9000");
		FileSystem fs = FileSystem.get(new URI("hdfs://mini1:9000"), conf, "hadoop");

		boolean delete = fs.delete(new Path("/aaa"), true);// true:�ݹ�ɾ��
		System.out.println(delete);
	}
}