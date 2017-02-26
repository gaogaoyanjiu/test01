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
 * �ͻ���ȥ����hdfsʱ������һ���û����ݵ�
 * Ĭ������£�hdfs�ͻ���api���jvm�л�ȡһ����������Ϊ�Լ����û����ݣ�-DHADOOP_USER_NAME=hadoop
 * 
 * Ҳ�����ڹ���ͻ���fs����ʱ��ͨ���������ݽ�ȥ
 * 
 * @author
 */
public class HdfsClientDemo {
	FileSystem fs = null;
	Configuration conf = null;

	// ��ʼ��
	@Before
	public void init() throws Exception {
		conf = new Configuration();// ������
		conf.set("fs.defaultFS", "hdfs://mini1:9000");

		// �õ�һ���ļ�ϵͳ�����Ŀͻ���ʵ������
		// fs = FileSystem.get(conf);//Ĭ���Ǳ����ļ�ϵͳ
		// ����1:uri(ָ����HDFS�ļ�ϵͳ���ϴ�),����2:ϵͳʵ��,����3:α����û�����
		fs = FileSystem.get(new URI("hdfs://mini1:9000"), conf, "hadoop");
	}

	// �ϴ��ļ�
	@Test
	public void testUpload() throws Exception {

		Thread.sleep(2000);
		fs.copyFromLocalFile(new Path("D:/my.log"), new Path("/my.log.copy"));
		fs.close();
	}

	// �����ļ�
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
			System.out.println(entry.getValue() + "--" + entry.getValue());// conf���ص�����
		}
	}

	/**
	 * ����Ŀ¼
	 */
	@Test
	public void makdirTest() throws Exception {
		boolean mkdirs = fs.mkdirs(new Path("/aaa/bbb"));
		System.out.println(mkdirs);
	}

	/**
	 * ɾ��
	 */
	@Test
	public void deleteTest() throws Exception {
		boolean delete = fs.delete(new Path("/"), true);// true�� �ݹ�ɾ��
		System.out.println(delete);
	}

	/**
	 * �鿴Ŀ¼��Ϣ��ֻ��ʾ�ļ�
	 * 
	 * @throws IOException
	 * @throws IllegalArgumentException
	 * @throws FileNotFoundException
	 */
	@Test
	public void testListFiles() throws FileNotFoundException, IllegalArgumentException, IOException {

		// ˼����Ϊʲô���ص�������������List֮�������
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

		while (listFiles.hasNext()) {
			LocatedFileStatus fileStatus = listFiles.next();
			System.out.println(fileStatus.getPath().getName());
			System.out.println(fileStatus.getBlockSize());
			System.out.println("Ȩ��:"+fileStatus.getPermission());
			System.out.println(fileStatus.getLen());
			BlockLocation[] blockLocations = fileStatus.getBlockLocations();
			for (BlockLocation bl : blockLocations) {
				System.out.println("��ĳ���:" + bl.getLength());
				System.out.println("���ƫ����:" + bl.getOffset());
				String[] hosts = bl.getHosts();
				for (String host : hosts) {
					System.out.println("������:"+host);
				}
			}
			System.out.println("--------------Ϊangelababy��ӡ�ķָ���--------------");
		}
	}

	/**
	 * �鿴�ļ����ļ�����Ϣ
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
			//������ļ������f------------
			if (fstatus.isFile())
				flag = "file-----------------";
			System.out.println(flag + fstatus.getPath().getName());
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://mini1:9000");
		// �õ�һ���ļ�ϵͳ�����Ŀͻ���ʵ������
		FileSystem fs = FileSystem.get(conf);

		fs.copyFromLocalFile(new Path("D:/my.log"), new Path("/my.log.copy"));
		fs.close();
	}
}