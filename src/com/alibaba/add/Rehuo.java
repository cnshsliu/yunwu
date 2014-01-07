package com.alibaba.add;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.log4j.Logger;

import com.aliyun.odps.Record;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.TableInfo;
import com.aliyun.odps.io.TableInputFormat;
import com.aliyun.odps.io.TableOutputFormat;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.mapreduce.JobClient;
import com.aliyun.odps.mapreduce.JobConf;
import com.aliyun.odps.mapreduce.MapContext;
import com.aliyun.odps.mapreduce.Mapper;
import com.aliyun.odps.mapreduce.ReduceContext;
import com.aliyun.odps.mapreduce.Reducer;

public class Rehuo {
	static int BUY = 0;
	static int CLICK = 1;
	static int BOOKMARK = 2;
	static int CART = 3;
	static int FOLLOW = 4;
	static int CART_LIVING_PERIOD = 60;
	static int INTEREST_LIVING_PERIOD = 60;
	static int CART_LIVING_WEEK = 8;
	static int INTEREST_LIVING_WEEK = 8;
	static float CART_TOP_SCORE = 60.0f;
	static float INTEREST_TOP_SCORE = 40.0f;
	static float MUST_BUY_SCORE = 60.0f;
	static float CLICK_TOP_SCORE = 12.0f;
	static float NORMAL_CLICK_WEIGHT = 1.0f;
	static int DAY_CLICK_CHECK_PERIOD = 7;
	static int DAY_CLICK_CHECK_THRESHOLD = 3;
	static long ms_24_hours = 86400000L;
	static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public static class RHMapper extends Mapper<Text, Text> {
		private Text userText = new Text();
		private Text infoText = new Text();

		public void map(LongWritable recordNum, Record record, MapContext<Text, Text> context) throws IOException, InterruptedException {
			Calendar dt = Calendar.getInstance();
			String user_id = record.get(0).toString();
			String brand_id = record.get(1).toString();
			String type = record.get(2).toString();
			// int type = Integer.parseInt(record.get(2).toString());
			String visit_datetime = record.get(3).toString();
			/*
			 * Date visit_date = null; try { visit_date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH).parse(visit_datetime); } catch (ParseException e) { e.printStackTrace(); } Calendar visit_cal = Calendar.getInstance(); visit_cal.setTime(visit_date);
			 */
			userText.set(user_id);
			infoText.set(brand_id + "$" + type + "$" + visit_datetime);
			try {
				dt.setTime(sdf.parse(visit_datetime));
				context.write(userText, infoText);
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
	}

	public static class Action {
		String brand = null;
		int types = -1;
		long ms = 0L;
		int age_m = -1;
		int age_w = -1;
		int age_d = -1;
		long age_ms = -1;

		public Action(String brandS, String typesS, String datetimeS) throws ParseException {
			Calendar baseT = Calendar.getInstance();
			Calendar actionT = Calendar.getInstance();
			baseT.setTime(sdf.parse("2013-07-03 00:00:00"));
			brand = brandS;
			types = Integer.valueOf(typesS).intValue();
			actionT.setTime(sdf.parse(datetimeS));
			ms = actionT.getTimeInMillis();
			/*
			 * int tt = 0; tt = baseT.get(Calendar.MONTH); tt = actionT.get(Calendar.MONTH); tt = baseT.get(Calendar.WEEK_OF_YEAR); tt = actionT.get(Calendar.WEEK_OF_YEAR); tt = baseT.get(Calendar.DAY_OF_YEAR); tt = actionT.get(Calendar.DAY_OF_YEAR);
			 */
			age_m = baseT.get(Calendar.MONTH) - actionT.get(Calendar.MONTH);
			age_w = baseT.get(Calendar.WEEK_OF_YEAR) - actionT.get(Calendar.WEEK_OF_YEAR);
			age_d = baseT.get(Calendar.DAY_OF_YEAR) - actionT.get(Calendar.DAY_OF_YEAR) - 1;
			age_ms = baseT.getTimeInMillis() - actionT.getTimeInMillis();

		}
	}

	public static class RHReducer extends Reducer<Text, Text> {
		private Record result = null;
		final static Logger logger = Logger.getLogger(RHReducer.class);

		@Override
		protected void setup(ReduceContext<Text, Text> context) throws IOException, InterruptedException {
			result = context.createOutputRecord();
		}

		int sumArr(int[] arr, int a, int b) {
			int ret = 0;
			if (b > arr.length)
				b = arr.length;
			for (int i = 0; i < b; i++) {
				ret += arr[a + i];
			}
			return ret;
		}

		int briefSumArr(int[] arr, int a, int b) {
			int ret = 0;
			if (b > arr.length)
				b = arr.length;
			for (int i = 0; i < b; i++) {
				if (arr[a + i] > 0)
					ret += 1;
			}
			return ret;
		}

		boolean everHappen(int[] arr, int a, int b) {
			boolean ret = false;
			b = (b > arr.length ? arr.length : b);
			for (int i = 0; i < b; i++) {
				if (arr[a + i] > 0) {
					ret = true;
					break;
				}
			}
			return ret;
		}

		float average(int[] arr, int a, int b) {
			int amount = 0;
			b = (b > arr.length ? arr.length : b);
			for (int i = 0; i < b; i++) {
				amount += arr[a + i];
			}
			return ((float) amount / (float) b);
		}

		int min(int[] arr, int a, int b) {
			int ret = arr[a];
			b = (b > arr.length ? arr.length : b);
			for (int i = 0; i < b; i++) {
				ret = (ret > arr[a + i] ? arr[a + i] : ret);
			}
			return ret;
		}

		int minWithout0(int[] arr, int a, int b) {
			int ret = arr[a];
			b = (b > arr.length ? arr.length : b);
			for (int i = 0; i < b; i++) {
				if (arr[a + i] == 0)
					continue;
				ret = (ret > arr[a + i] ? arr[a + i] : ret);
			}
			return ret;
		}

		int max(int[] arr, int a, int b) {
			int ret = arr[a];
			b = (b > arr.length ? arr.length : b);
			for (int i = 0; i < b; i++) {
				ret = (ret < arr[a + i] ? arr[a + i] : ret);
			}
			return ret;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see com.alimr_in mr_outyun.odps.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, com.aliyun.odps.mapreduce.ReduceContext)
		 */
		@Override
		public void reduce(Text key, Iterable<Text> values, ReduceContext<Text, Text> context) throws IOException, InterruptedException {
			Calendar t0702 = Calendar.getInstance();
			long l0702 = 0L;
			Set<String> brands = new HashSet<String>();
			Map<String, Vector<Action>> brandActions = new HashMap<String, Vector<Action>>();
			try {
				t0702.setTime(sdf.parse("2013-07-01 12:00:00"));
				l0702 = t0702.getTimeInMillis();
				for (Text val : values) {
					String infoString = val.toString();
					StringTokenizer st = new StringTokenizer(infoString, "$");
					String brandS = st.nextToken();
					brands.add(brandS);
					Action act = new Action(brandS, st.nextToken(), st.nextToken());
					if (brandActions.containsKey(brandS)) {
						brandActions.get(brandS).add(act);
					} else {
						Vector<Action> v = new Vector<Action>();
						v.add(act);
						brandActions.put(brandS, v);
					}
				}

				// 找出用户购买前的平均点击次数。
				int clickTotal = 0;
				int clickTimes = 0;
				int minClickBeforeBuy = 1;
				for (Iterator<String> itr = brands.iterator(); itr.hasNext();) {
					String brand = (String) itr.next();
					for (Iterator<Action> it = brandActions.get(brand).iterator(); it.hasNext();) {
						Action act = (Action) it.next();
						if (act.types == BUY) {
							int clickCount = 0;
							for (Iterator<Action> it2 = brandActions.get(brand).iterator(); it2.hasNext();) {
								Action act2 = (Action) it2.next();
								if (act2.types == CLICK && (act2.ms <= act.ms && act2.ms >= act.ms - 43200000)) { // 12小时
									clickCount++;
								}
							}
							if (clickCount > 0) {
								clickTimes++;
								clickTotal += clickCount;
								minClickBeforeBuy = minClickBeforeBuy > clickCount ? clickCount : minClickBeforeBuy;
							}
						}
					}
				}

				int averageClickBeforeBuy = clickTotal / clickTimes;

				int brand_selected_count = 0;
				StringBuilder brand_selected_sb = new StringBuilder();
				boolean everBuySomething = false;
				boolean everInterestSomething = false;
				for (Iterator<String> itr = brands.iterator(); itr.hasNext();) {
					String brand = (String) itr.next();

					int[] month_buy_mark = { 0, 0, 0, 0, 0 };
					int[] month_interest_mark = { 0, 0, 0, 0, 0 };
					int[] month_cart_mark = { 0, 0, 0, 0, 0 };
					int[] week_buy_mark = { 0, 0, 0, 0, 0 };
					int[] day_buy_mark = { 0, 0, 0, 0, 0, 0, 0 };
					int[] month_click_mark = { 0, 0, 0, 0, 0 };
					int[] week_click_mark = { 0, 0, 0, 0, 0 };
					int[] day_click_mark = { 0, 0, 0, 0, 0, 0, 0 };
					int buy_in_last12 = 0;
					int click_in_last12 = 0;
					long lastCartMs = -1L;
					long lastBuyMs = -1L;
					long lastInterestMs = -1L;
					int lastInterestAgeW = 1;
					int lastInterestAgeD = -1;
					int lastCartAgeM = 1;
					int lastCartAgeD = -1;
					float clickWeight = NORMAL_CLICK_WEIGHT;
					float score = (float) 0.0;
					float clickScore = (float) 0.0;

					for (Iterator<Action> it = brandActions.get(brand).iterator(); it.hasNext();) {
						Action act = (Action) it.next();
						if (act.types == BUY) {
							month_buy_mark[act.age_m]++;

							if (act.age_w < 5) {
								week_buy_mark[act.age_w]++;
							}

							if (act.age_d < 7) {
								day_buy_mark[act.age_d]++;
							}
							lastBuyMs = act.ms > lastBuyMs ? act.ms : lastBuyMs;
							clickWeight = NORMAL_CLICK_WEIGHT * 1.5f;
							everBuySomething = true;
							if (act.ms >= l0702) {
								buy_in_last12++;
							}

						} else if (act.types == CART) {
							month_cart_mark[act.age_m]++;
							if (act.ms > lastCartMs) {
								lastCartMs = act.ms;
								lastCartAgeM = act.age_m;
								lastCartAgeD = act.age_d;
							}
							clickWeight = NORMAL_CLICK_WEIGHT * 1.5f;
							everInterestSomething = true;
						} else if (act.types == FOLLOW || act.types == BOOKMARK) {
							month_interest_mark[act.age_m]++;
							if (act.ms > lastInterestMs) {
								lastInterestMs = act.ms;
								lastInterestAgeW = act.age_w;
								lastInterestAgeD = act.age_d;
							}
							clickWeight = NORMAL_CLICK_WEIGHT * 1.5f;
							everInterestSomething = true;
						} else if (act.types == CLICK) {
							month_click_mark[act.age_m]++;

							if (act.age_w < 5) {
								week_click_mark[act.age_w]++;
							}

							if (act.age_d < 7) {
								day_click_mark[act.age_d]++;
							}

							clickScore += (float) (CLICK_TOP_SCORE * clickWeight) / (act.age_d + 1);
							if (act.ms >= l0702) {
								click_in_last12++;
							}
						}
					}

					if (briefSumArr(month_buy_mark, 1, 2) == 2) {
						if (month_buy_mark[0] < Math.min(month_buy_mark[1], month_buy_mark[2]))
							score += MUST_BUY_SCORE;
					}

					if (briefSumArr(week_buy_mark, 1, 2) >= 2) {
						if (week_buy_mark[0] < Math.min(week_buy_mark[1], week_buy_mark[2]))
							score += MUST_BUY_SCORE;
					}
					if (briefSumArr(day_buy_mark, 0, 3) >= 3) {
						score += MUST_BUY_SCORE;
					}
					if (lastCartMs > -1L) {
						int tmpweek = lastCartAgeD / 7;
						if (tmpweek <= CART_LIVING_WEEK) {
							if (lastBuyMs < lastCartMs) { // 加车后没买过
								score += CART_TOP_SCORE / (1.0f + 1.0f / CART_LIVING_WEEK * tmpweek);
							} else if (month_buy_mark[0] < month_buy_mark[1]) {
								score += CART_TOP_SCORE / (1.0f + 1.0f / CART_LIVING_WEEK * tmpweek);
							}
						}
					}
					if (lastInterestMs > -1L) {
						int tmpweek = lastInterestAgeD / 7;
						if (tmpweek <= INTEREST_LIVING_WEEK) {
							if (lastBuyMs < lastInterestMs) {
								score += INTEREST_TOP_SCORE / (1.0f + 1.0f / INTEREST_LIVING_WEEK * tmpweek);
							} else if (month_buy_mark[0] < month_buy_mark[1]) {
								score += INTEREST_TOP_SCORE / (1.0f + 1.0f / INTEREST_LIVING_WEEK * tmpweek);
							}
						}
					}

					for (int i = 0; i < DAY_CLICK_CHECK_PERIOD; i++) {
						if (day_click_mark[i] >= DAY_CLICK_CHECK_THRESHOLD + i) {
							boolean alreadyBought = false;
							for (int j = 0; j <= i; j++) {
								if (day_buy_mark[j] >= 1) {
									alreadyBought = true;
									break;
								}
							}
							if (!alreadyBought) {
								score += MUST_BUY_SCORE / (i + 1);
								if (score >= MUST_BUY_SCORE) {
									break;
								}
							}
						}
					}

					// 曾经买过的，如果6月份有点，则还会买
					if (briefSumArr(month_buy_mark, 1, 4) > 2) {
						if (month_buy_mark[0] < minWithout0(month_buy_mark, 1, 4))
							score += MUST_BUY_SCORE;
					}

					// 6原份关注过但6月份没有买的，7月份会买
					if (month_interest_mark[1] > 0) {
						if (briefSumArr(month_buy_mark, 0, 2) == 0) {
							score += MUST_BUY_SCORE;
						}
					}

					// 找出用户点击到购买的操作习惯
					if (day_click_mark[0] >= averageClickBeforeBuy && day_buy_mark[0] == 0) {
						score += MUST_BUY_SCORE;
					}
					if (day_click_mark[1] > averageClickBeforeBuy && day_buy_mark[0] == 0) {
						score += MUST_BUY_SCORE;
					}
					if (click_in_last12 >= averageClickBeforeBuy && buy_in_last12 == 0) {
						score += MUST_BUY_SCORE;
					}
					// 六，七月份的interest, 7月份没买的，都买掉。
					// todo: 后续优化，看用户interest到buy的转化率
					if (month_interest_mark[0] > 0 && month_buy_mark[0] == 0) {
						score += MUST_BUY_SCORE;
					}
					if (month_interest_mark[1] > 0 && sumArr(month_buy_mark, 0, 2) == 0) {
						score += MUST_BUY_SCORE;
					}

					score += clickScore;

					if (score >= MUST_BUY_SCORE) {
						if (brand_selected_count > 0) {
							brand_selected_sb.append(",");
						}
						brand_selected_sb.append(brand);
						brand_selected_count++;
					}

				}

				if (brand_selected_count > 0 && (everBuySomething || everInterestSomething)) {
					Text result_brand = new Text();
					result_brand.set(brand_selected_sb.toString());
					result.set(0, key);
					result.set(1, result_brand);
					context.write(result);
				}

			} catch (Exception ex) {
				logger.error(ex.getLocalizedMessage());
			} finally {
				brandActions = null;
				brands = null;
			}
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: wordcount <in_table> <out_table>");
			System.exit(2);
		}
		JobConf job = new JobConf();
		job.setMapperClass(RHMapper.class);
		job.setReducerClass(RHReducer.class);
		job.setNumReduceTasks(100);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		TableInputFormat.addInput(new TableInfo(args[0]), job);
		TableOutputFormat.addOutput(new TableInfo(args[1]), job);
		JobClient.runJob(job);

	}
}
