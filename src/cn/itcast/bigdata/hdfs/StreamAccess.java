package cn.itcast.bigdata.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

/**
 * �����Щ��װ�õķ������Եĸ��ײ�һЩ�Ĳ�����ʽ
 * �ϲ���Щmapreduce��spark�������ܣ�ȥhdfs�л�ȡ���ݵ�ʱ�򣬾��ǵ������ֵײ��api
 * @author
 *
 */
public class StreamAccess {
	
	FileSystem fs = null;

	@Before
	public void init() throws Exception {
		Configuration conf = new Configuration();
		fs = FileSystem.get(new URI("hdfs://mini1:9000"), conf, "hadoop");
	}
	
	/**
	 * ͨ�����ķ�ʽ�ϴ��ļ���hdfs
	 * @throws Exception
	 */
	@Test
	public void testUpload() throws Exception {
		
		FileInputStream in = new FileInputStream("d:/angelababy.love");
		FSDataOutputStream out = fs.create(new Path("/angelababy.love"), true);
		
		IOUtils.copy(in, out);
	}
	
	/**
	 * ��ʾhdfs���ļ�������
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void testCat() throws IllegalArgumentException, IOException{
		
		FSDataInputStream in = fs.open(new Path("/angelababy.love"));
		
		IOUtils.copy(in, System.out);
	}
	/**
	 * hdfs֧�������λ�����ļ���ȡ�����ҿ��Է���ض�ȡָ������
	 * �����ϲ�ֲ�ʽ�����ܲ�����������
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testRandomAccess() throws IllegalArgumentException, IOException{
		//�Ȼ�ȡһ���ļ���������----���hdfs�ϵ�
		FSDataInputStream in = fs.open(new Path("/angelababy.love"));
		
		//���Խ�������ʼƫ���������Զ���
		in.seek(12);
		
		//�ٹ���һ���ļ��������----��Ա��ص�
		FileOutputStream out = new FileOutputStream(new File("D:/angelababy.love"));
		
		IOUtils.copy(in,out);
	}
}
