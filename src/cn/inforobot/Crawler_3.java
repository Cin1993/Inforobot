package cn.inforobot;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;

import cn.inforobot.dao.Dao;
import cn.inforobot.database.DBUtil;
import cn.inforobot.pojo.Host_configure;

public class Crawler_3 {
	private static Dao dao = new Dao();

	public static void main(String[] args) {
		//chrome浏览器的驱动设置
		// System.setProperty("webdriver.chrome.driver",
		// "C:\\chromedriver.exe");
		//统计变量，用来计算程序运行的次数
		int round = 0;
		//死循环，保证程序一直运行
		while (true) {
			//获取存放抓取网站的信息
			List<Host_configure> host_configure = dao.getHostConfigure();
			//数据库连接设置
			ResultSet rs = null;
			PreparedStatement pstmt = null;
			Connection conn = null;
			// 连接info_rob数据库
			conn = DBUtil.getConn("info_rob");

			//获取抓取配置信息
			String sql = "select * from class_detail where crawl_depth = '2'";
			// map存放class_details 3/5列的内容
			Map<String, String> map = new HashMap<String, String>();
			try {
				pstmt = conn.prepareStatement(sql);
				rs = pstmt.executeQuery();
				while (rs.next()) {
					map.put(rs.getString(3), rs.getString(5));
				}
				// 存放需要抓取的关键词
				String keyword = getKeyword();
				// 抓取的关键词循环
				while (!keyword.equals("")) {
					// 抓取网站的循环
					for (int i = 0; i < host_configure.size(); i++) {
						// WebDriver driver = new ChromeDriver();
						// 建立FireFox浏览器的驱动
						WebDriver driver = new FirefoxDriver();
						// 字符串拼出抓取的关键词的网页的url
						String url = host_configure.get(i).getSearch_box_url() + keyword;
						// 浏览器跳转到url
						driver.get(url);
						// 使用Selenium集成的getPageSource()方法获取网页的源代码，并使用Jsoup转为Document类型，方便解析
						Document doc0 = Jsoup.parse(driver.getPageSource());
						// 使用Jsoup解析出翻页的元素
						Element nextpage = doc0.select("a[id=pagnNextLink]").first();
						// 该变量用来统计抓取商品的数量
						int good_amount = 1;
						// 翻页循环
						while (!(nextpage == null || nextpage.toString().equals(""))) {
							// 浏览器跳转到搜索结果页面
							driver.get(url);
							// 使用Selenium集成的getPageSource()方法获取网页的源代码，并使用Jsoup转为Document类型，方便解析
							Document doc = Jsoup.parse(driver.getPageSource());
							// 解析出每个商品的具体信息
							Elements es = doc.select("ul[id=s-results-list-atf]>li");
							// 解析出翻页的元素
							nextpage = doc.select("a[id=pagnNextLink]").first();
							// 判断是否包含翻页元素，确定是否是最后一页
							if (!(nextpage == null || nextpage.toString().equals(""))) {
								// 字符串分割出下一页的url
								url = nextpage.toString().split("href=\"")[1].split("\"")[0];
								// 拼出完整的url，为翻页浏览器跳转做准备
								url = "http://www.amazon.com" + url;
							}
							// 商品列表的迭代器
							Iterator<Element> listIterator = es.iterator();
							// 临时变量
							String temp = "";
							// 存放每个具体商品的url
							String goods_url = "";
							// WebDriver goods_driver = new ChromeDriver();
							// 创建新的浏览器用于开启商品的详情页面
							WebDriver goods_driver = new FirefoxDriver();
							// 具体商品循环
							while (listIterator.hasNext()) {
								// 为获取商品的img_url和url做准备
								Element e = listIterator.next();
								// 声明商品的map
								Map<String, String> goods_map = new HashMap<String, String>();
								temp = e.getElementsByAttribute("href").toString();
								if (temp.contains("href=\"")) {
									//截取出商品的url
									goods_url = temp.split("href=\"")[1].split("\"")[0];
									//判断是否是合法的url
									if ((goods_url.startsWith("http://") || goods_url.startsWith("https://"))
											&& baohan(goods_url, keyword)) {
										System.out.println(
												"************************************************************");
										System.out.println(
												"*                                                          *");
										System.out.println("          the goods is number: " + good_amount);
										System.out.println(
												"*                                                          *");
										System.out.println(
												"************************************************************");
										System.out.println("");
										System.out.println("the goods url is " + goods_url);
										System.out.println("\n\n");
										// 解析出商品的img_url
										String img_url = e.select("img[src]").first().toString();
										System.out.println("the goods img_url is " + img_url);
										System.out.println("\n\n");
										// 分割出商品的img_url
										img_url = img_url.split("src=\"")[1].split("\"")[0];

										// 寤舵椂璁剧疆
										// java.util.concurrent.TimeUnit.SECONDS.sleep((int)
										// (Math.random() * 3 + 2));

										// 浏览器跳转到商品的详情页面
										goods_driver.navigate().to(goods_url);
										// 获取商品详情页面的源代码
										String goodssrc = goods_driver.getPageSource();
										// 将商品详情页面的源代码转为Document类型
										Document doc_goods = Jsoup.parse(goodssrc);

										// 将商品页面的商品detail存储到商品的map中
										goods_map = getDetail(map, doc_goods, keyword);
										// 向商品map中添加商品页面的源代码
										goods_map.put("src", goodssrc);
										// 获取商品的新旧状况，并存放到goods_map中
										if (goods_map.get("condition") != null) {
											if (goods_map.get("condition").contains("new"))
												goods_map.put("condition", "used");
											else {
												goods_map.put("condition", "new");
											}
										} else {
											goods_map.put("condition", "new");
										}
										// 扩充商品的detail，将商品的title添加到detail中
										goods_map.put("detail", goods_map.get("detail") + goods_map.get("title"));
										// 将商品的url添加到goods_map
										goods_map.put("url", goods_url);
										// 将商品的img_url添加到goods_map
										goods_map.put("img_url", img_url);

										//存放map的键
										String s1 = "";
										//存放map的值
										String s2 = "";
										// 声明goods_map的迭代器
										Iterator<Map.Entry<String, String>> entries = goods_map.entrySet().iterator();
										// 循环
										while (entries.hasNext()) {
											Map.Entry<String, String> entry = entries.next();
											if (entries.hasNext()) {
												s1 = s1 + "`" + entry.getKey() + "`" + ", ";
												s2 = s2 + "'" + entry.getValue().replaceAll("'", "''") + "', ";
											} else {
												s1 = s1 + "`" + entry.getKey() + "`";
												s2 = s2 + "'" + entry.getValue().replaceAll("'", "''") + "'";
											}
										}
										// 将sql语句中的“\”替换为“\\”防止执行sql语句的时候报错
										s2 = s2.replaceAll("\\\\", "\\\\\\\\");
										// 插入商品数据
										try {
											pstmt.execute("insert into goods" + "(" + s1 + ") value(" + s2 + ")");
											System.out.println(
													"@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
											System.out.println(
													"@                                                          @");
											System.out.println(
													"@                 insert data has finished                 @");
											System.out.println(
													"@                                                          @");
											System.out.println(
													"@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");

											// classification();
											// System.out.println("******get the
											// attribute has finished******");
										} catch (Exception e2) {
											// TODO: handle exception
											//如果报错，将关键词的state字段设置为E
											setERRORStatus(keyword);
											e2.printStackTrace();
										}
										// 统计商品数量的变量自加
										good_amount++;
									}
								}
							}
							// 关闭商品详情页面的FireFox
							goods_driver.quit();
							// 判断翻页元素是否可以点击
							if (driver.findElement(By.id("pagnNextString")).isEnabled())
								// 点击翻页元素实现翻页功能
								driver.findElement(By.id("pagnNextString")).click();
						}
						System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
						System.out.println("^                                                          ^");
						System.out.println("         the keyword: " + keyword + " has finished              ");
						System.out.println("^                                                          ^");
						System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
						// 关键词抓取完毕，退出FireFox浏览器
						driver.quit();
					}
					// 将关键词的state字段设置为Y，表明该关键词抓取完毕
					pstmt.execute("UPDATE keyword SET state='Y' WHERE key_word = '" + keyword + "'");
					// classification();
					// 获取下一个需要抓取的关键词
					keyword = getKeyword();
				}
				// 关闭数据库连接
				DBUtil.closeConn(rs, pstmt, conn);
				System.out.println("\n\n\n");
				System.out.println("$$$$$      WARNING:There is nothing to crawl!!!      $$$$$");
				System.out.println("\n\n\n");
			} catch (Exception e) {
				e.printStackTrace();
				// 关闭数据库连接
				DBUtil.closeConn(rs, pstmt, conn);
			}
			System.out.println("\n\n\n");
			System.out.println("$$$$$      MESSAGE:Start crawl price again      $$$$$");
			System.out.println("\n\n\n");
			try {
				//抓取隐藏到购物车中的价格
				getCartPrice(noPrice());
				//抓取一些price价格不匹配的
				getBuyboxPrice(noPrice());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//打印程序执行次数的信息
			System.out.println("*********         round:" + round + "        **********");
			System.out.println("--------------------------END--------------------------");
			System.out.println("\n\n\n\n\n");
			try {
				//程序执行完1次，休眠2分钟，然后执行下一次
				java.util.concurrent.TimeUnit.SECONDS.sleep(120);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	
	/**
	 * 抓取隐藏到购物车中的价格
	 * @param：未获取价格的商品的urlList
	 * */
	
	
	public static void getCartPrice(ArrayList<String> noprice_list) throws Exception {
		// 声明FireFoxDriver
		WebDriver driver = new FirefoxDriver();
		// 数据库连接设置
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		Connection conn = null;
		// 连接info_rob数据库
		conn = DBUtil.getConn("info_rob");
		By by = By.linkText("See price in cart");
		for (int i = 0; i < noprice_list.size(); i++) {
			driver.get(noprice_list.get(i));
			if (IsElementPresent(driver, by)) {
				driver.findElement(by).click();
				java.util.concurrent.TimeUnit.SECONDS.sleep((int) (Math.random() * 3 + 1));
				WebElement webe = driver.findElement(By.id("priceblock_ourprice"));
				pstmt = conn.prepareStatement("");
				System.out.println(
						"UPDATE goods SET price='" + webe.getText() + "' WHERE url = '" + noprice_list.get(i) + "'");
				pstmt.execute(
						"UPDATE goods SET price='" + webe.getText() + "' WHERE url = '" + noprice_list.get(i) + "'");
			} else {
				System.out.println("------------the format is not matched---------------------");
			}
		}
		driver.quit();
		DBUtil.closeConn(rs, pstmt, conn);
	}
	
	

	/**
	 * 从网页右侧的BuyBox，抓取一些price价格不匹配的
	 * @param：未获取价格的商品的urlList
	 * */
	public static void getBuyboxPrice(ArrayList<String> noprice_list) throws Exception {
		String sql = "";
		WebDriver driver = new FirefoxDriver();
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		Connection conn = null;
		conn = DBUtil.getConn("info_rob");
		for (int i = 0; i < noprice_list.size(); i++) {
			driver.get(noprice_list.get(i));
			Document doc = Jsoup.parse(driver.getPageSource());
			if (!doc.select("span[class=a-size-medium a-color-price offer-price a-text-normal]").equals("")) {
				Elements e = doc.select("span[class=a-size-medium a-color-price offer-price a-text-normal]");
				System.out.println(e);
				sql = "UPDATE goods SET price='" + e.text() + "' WHERE url = '" + noprice_list.get(i) + "'";
				pstmt = conn.prepareStatement("");
				System.out.println(sql);
				pstmt.execute(sql);
				System.out.println("==========================================================================");
			} else if (!doc.select("span[class=a-size-base a-color-price offer-price a-text-normal]").equals("")) {
				Elements e = doc.select("span[class=a-size-base a-color-price offer-price a-text-normal]");
				System.err.println(e);
				sql = "UPDATE goods SET price='" + e.text() + "' WHERE url = '" + noprice_list.get(i) + "'";
				System.out.println(sql);
				pstmt = conn.prepareStatement("");
				pstmt.execute(sql);
				System.out.println("==========================================================================");
			} else if (!doc.select("span[class=a-size-base-plus]").equals("")) {
				Elements e = doc.select("span[class=a-size-base-plus]");
				String s = "";
				if (e.text().contains("$")) {
					s = "$" + e.text().split("$")[1];
					sql = "UPDATE goods SET price='" + s + "' WHERE url = '" + noprice_list.get(i) + "'";
					System.out.println(sql);
					pstmt = conn.prepareStatement(sql);
					pstmt.execute();
					System.out.println("==========================================================================");
				}
			} else if (!doc.select("span[class=a-color-price]").equals("")) {
				Elements e = doc.select("span[class=a-color-price]");
				String s = "";
				if (e.text().contains("$")) {
					s = "$" + e.text().split("$")[1];
					sql = "UPDATE goods SET price='" + s + "' WHERE url = '" + noprice_list.get(i) + "'";
					System.out.println(sql);
					pstmt = conn.prepareStatement("");
					pstmt.execute(sql);
				}
			}
		}
		driver.quit();
		DBUtil.closeConn(rs, pstmt, conn);
	}

	// 获取数据库中没有price的商品的url
	public static ArrayList<String> noPrice() {
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		Connection conn = null;
		ArrayList<String> noprice_list = new ArrayList<String>();
		// 杩炴帴info_rob鏁版嵁搴�
		conn = DBUtil.getConn("info_rob");
		String sql = "select url from goods where price = ''";
		try {
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				noprice_list.add(rs.getString(1));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			DBUtil.closeConn(rs, pstmt, conn);
		}
		return noprice_list;
	}

	// 将keyword的state字段设置为E
	public static void setERRORStatus(String keyword) {
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		Connection conn = null;
		conn = DBUtil.getConn("info_rob");
		String sql = "UPDATE keyword SET state='E' WHERE key_word = '" + keyword + "'";
		try {
			pstmt = conn.prepareStatement("");
			pstmt.execute(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		DBUtil.closeConn(rs, pstmt, conn);
	}

	// 处理关键词中包含20%的情况
	public static boolean baohan(String url, String keyword) {
		String split[] = null;
		boolean flag = true;
		if (keyword.contains("%20")) {
			split = keyword.split("%20");
			for (int i = 0; i < split.length; i++) {
				if (!url.toLowerCase().contains(split[i].toLowerCase()))
					flag = false;
			}
		} else {
			if (!url.toLowerCase().contains(keyword.toLowerCase()))
				flag = false;
		}
		return flag;
	}

	// 获取商品的详细信息
	public static Map<String, String> getDetail(Map<String, String> map, Document doc_goods, String key_word) {

		Map<String, String> info_map = new HashMap<String, String>();
		System.out.println(doc_goods.select("ul[class=a-horizontal a-size-small]>li").text());
		for (Map.Entry<String, String> entry : map.entrySet()) {
			Elements es = doc_goods.select(entry.getValue());
			info_map.put(entry.getKey(), es.text().replaceAll("'", "''"));
		}
		System.out.println(info_map);
		return info_map;
	}

	// 获取需要抓取的关键词
	public static String getKeyword() {
		String keyword = "";
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		Connection conn = null;
		// 连接到info_rob数据库
		conn = DBUtil.getConn("info_rob");
		String sql = "select * from keyword where isnull(state) or state='' or state='E' or (state='R' and robot_name='"
				+ getComputerName() + "')";
		try {
			pstmt = conn.prepareStatement("");
			rs = pstmt.executeQuery(sql);
			// 获取关键词
			if (rs.next()) {
				keyword = rs.getString(1);
			}
			// 更新数据库中关键词的状态
			if (!keyword.equals("")) {
				sql = "UPDATE keyword SET state='R',robot_name='" + getComputerName() + "' WHERE key_word = '" + keyword
						+ "'";
				pstmt.execute(sql);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// 关闭数据库的连接
		DBUtil.closeConn(rs, pstmt, conn);
		return keyword;
	}
	
	
	// 判断网页是否包含某个元素
	public static boolean IsElementPresent(WebDriver driver, By by) {
		try {
			driver.findElement(by);
			return true;
		} catch (Exception e) {
			return false;
		}
	}
	
	//获取计算机名称
	public static String getComputerName() {
		String computerName = System.getenv().get("COMPUTERNAME");
		return computerName;
	}

	// 解析商品的分类
	public static void classification() {
		Connection conn = DBUtil.getConn("info_rob");
		PreparedStatement ps = null;
		PreparedStatement ps1 = null;
		ResultSet rs = null;
		Set<String> set = new HashSet<String>();
		try {
			ps = conn.prepareStatement("");
			ps1 = conn.prepareStatement("");
			rs = ps.executeQuery("select distinct class from class_attribute");
			while (rs.next()) {
				set.add(rs.getString(1));
			}
			rs = ps.executeQuery(
					"select class_data,url from goods where isnull(class_name) or class_name='' or class_name='unknown_class' or class_name like '%(unclassfied)%'");
			while (rs.next()) {
				String s = rs.getString("class_data");
				String url = rs.getString("url");
				String flag = "";
				for (String x : set) {
					String sc = s;
					if (!x.contains("&")) {
						while (sc.contains("&") && sc.contains("鈥�")) {
							// sc = sc.substring(s.indexOf("鈥�") + 1);
							if (s.lastIndexOf("&") > s.lastIndexOf("鈥�"))
								sc = "";
							else
								sc = sc.substring(s.indexOf("鈥�", s.lastIndexOf("&")) + 1);
						}
					}
					if (sc.contains(x)) {
						ps1.execute("update goods set class_name='" + x + "' where url='" + url + "'");
						System.out.println(x);
						flag = "Y";
						break;
					}
				}
				if (!flag.equals("Y")) {
					if (s.contains("鈥�")) {
						s = s.substring(s.lastIndexOf("鈥�") + 1) + "(undefined)";
					}
					while (s.startsWith(" ")) {
						s = s.replace(" ", "");
					}
					System.out.println(s);
					ps1.execute("update goods set class_name='" + s + "' where url='" + url + "'");
				}
			}
			ps1 = null;
			DBUtil.closeConn(rs, ps, conn);
		} catch (Exception e) {
			e.printStackTrace();
			DBUtil.closeConn(rs, ps, conn);
		}
	}

}
