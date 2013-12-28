package com.alibaba.add;

import java.io.IOException;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.add.WordCount.WCMapper;
import com.alibaba.add.WordCount.WCReducer;
import com.aliyun.odps.Record;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.TableInfo;
import com.aliyun.odps.io.TableInputFormat;
import com.aliyun.odps.io.TableOutputFormat;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.JobConf;
import com.aliyun.odps.mapreduce.unittest.KeyValue;
import com.aliyun.odps.mapreduce.unittest.MRUnitTest;
import com.aliyun.odps.mapreduce.unittest.MapOutput;
import com.aliyun.odps.mapreduce.unittest.MapUTContext;
import com.aliyun.odps.mapreduce.unittest.ReduceOutput;
import com.aliyun.odps.mapreduce.unittest.ReduceUTContext;


public class WordCountTest extends MRUnitTest<Text, LongWritable> {
	// 定义输入输出表的 schema
	private final static String INPUT_SCHEMA = "a:string";
	private final static String OUTPUT_SCHEMA = "k:string,v:bigint";
	private JobConf job;

	public WordCountTest() throws IOException {
		// 准备作业配置
		job = new JobConf();
		job.setMapperClass(WCMapper.class);
		job.setReducerClass(WCReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		TableInputFormat.addInput(new TableInfo("mr_wc_in"), job);
		TableOutputFormat.addOutput(new TableInfo("mr_wc_out"), job);
	}

	@Test
	public void TestMap() throws IOException, ClassNotFoundException, InterruptedException {
		// 准备测试数据
		MapUTContext context = new MapUTContext();
		context.setInputSchema(INPUT_SCHEMA);
		context.setOutputSchema(OUTPUT_SCHEMA);
		Record record = context.createInputRecord();
		record.set(new Text[] { new Text("hello java. hello python. ") });
		context.addInputRecord(record);

		// 运行 map 过程
		MapOutput<Text, LongWritable> output = runMapper(job, context);

		// 验证 map 的结果，为6组 key/value 对
		List<KeyValue<Text, LongWritable>> rawKvs = output.getRawOutputKeyValues();
		Assert.assertEquals(4, rawKvs.size());
		Assert.assertEquals(new KeyValue<Text, LongWritable>(new Text("hello"), new LongWritable(1)), rawKvs.get(0));
		Assert.assertEquals(new KeyValue<Text, LongWritable>(new Text("java"), new LongWritable(1)), rawKvs.get(1));
		Assert.assertEquals(new KeyValue<Text, LongWritable>(new Text("hello"), new LongWritable(1)), rawKvs.get(2));
		Assert.assertEquals(new KeyValue<Text, LongWritable>(new Text("python"), new LongWritable(1)), rawKvs.get(3));
	}

	@Test
	public void TestReduce() throws IOException, ClassNotFoundException, InterruptedException {
		// 准备测试数据
		ReduceUTContext<Text, LongWritable> context = new ReduceUTContext<Text, LongWritable>();
		context.setOutputSchema(OUTPUT_SCHEMA);
		context.addInputKeyValue(new Text("hello"), new LongWritable(1));
		context.addInputKeyValue(new Text("java"), new LongWritable(1));
		context.addInputKeyValue(new Text("hello"), new LongWritable(1));
		context.addInputKeyValue(new Text("python"), new LongWritable(1));
		// 运行 reduce 过程
		ReduceOutput output = runReducer(job, context);
		List<Record> records = output.getOutputRecords();
		// 验证 reduce 结果，为 3 条 record Assert.assertEquals(3, records.size()); Assert.assertEquals(new Text("hello"), records.get(0).get("k")); Assert.assertEquals(new LongWritable(2), records.get(0).get("v")); Assert.assertEquals(new Text("java"), records.get(1).get("k")); Assert.assertEquals(new LongWritable(1), records.get(1).get("v")); Assert.assertEquals(new Text("python"), records.get(2).get("k")); Assert.assertEquals(new LongWritable(1), records.get(2).get("v"));
	}
}
