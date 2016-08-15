package cn.inforobot;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
import org.openqa.selenium.firefox.FirefoxDriver;
import cn.inforobot.dao.Dao;
import cn.inforobot.database.DBUtil;
import cn.inforobot.pojo.Host_configure;

public class Crawler_3 {
	private static Dao dao = new Dao();

	public static void main(String[] args) {
		// System.setProperty("webdriver.chrome.driver",
		// "C:\\chromedriver.exe");
		// 获取搜索网站的List
		List<Host_configure> host_configure = dao.getHostConfigure();
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		Connection conn = null;
		// 连接info_rob数据库
		conn = DBUtil.getConn("info_rob");

		String sql = "select * from class_detail where crawl_depth = '2'";
		// String keyword = "";
		// 存放class_details表格3/5列的内容
		Map<String, String> map = new HashMap<String, String>();
		try {
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				map.put(rs.getString(3), rs.getString(5));
			}
			// 获取首次抓取的关键词
			String keyword = getKeyword();
			// 搜索关键词的循环
			while (!keyword.equals("")) {
				// 搜索网站的循环
				for (int i = 0; i < host_configure.size(); i++) {
					// WebDriver driver = new ChromeDriver();
					// 启动FireFox
					WebDriver driver = new FirefoxDriver();
					// 字符串拼出查询商品的url
					String url = host_configure.get(i).getSearch_box_url() + keyword;
					// 跳转到商品的url
					driver.get(url);
					// 使用Selenium获取网页的源代码，并存储为Jsoup的Document类型
					Document doc0 = Jsoup.parse(driver.getPageSource());
					// 通过Jsoup的选择器，选择出翻页的标签
					Element nextpage = doc0.select("a[id=pagnNextLink]").first();
					// 统计抓取到的商品的数量
					int good_amount = 1;
					// 翻页
					while (!(nextpage == null || nextpage.toString().equals(""))) {
						// 跳转到商品的页面
						driver.get(url);
						// 获取商品页面的源代码
						Document doc = Jsoup.parse(driver.getPageSource());
						// 移除页面的js代码
						// doc.select("script").remove();
						// 通过select选择器，选择出查询到商品的列表
						Elements es = doc.select("ul[id=s-results-list-atf]>li");
						// 获取翻页标识
						nextpage = doc.select("a[id=pagnNextLink]").first();
						// 判断翻页标识是否为空，以确定是否到达最后一页
						if (!(nextpage == null || nextpage.toString().equals(""))) {
							// 截取翻页后的页面的url
							url = nextpage.toString().split("href=\"")[1].split("\"")[0];
							// 字符串拼凑出翻页后的url
							url = "http://www.amazon.com" + url;
						}
						// 存放商品的url的迭代器
						Iterator<Element> listIterator = es.iterator();
						// 临时字符串
						String temp = "";
						// 存放商品页面的url
						String goods_url = "";
						// WebDriver goods_driver = new ChromeDriver();
						// 开启FireFox打开商品页面
						WebDriver goods_driver = new FirefoxDriver();
						// 循环获取迭代器中的商品
						while (listIterator.hasNext()) {
							// 用于获取商品的img_url和url的迭代器
							Element e = listIterator.next();
							// 存放商品信息的Map
							Map<String, String> goods_map = new HashMap<String, String>();
							temp = e.getElementsByAttribute("href").toString();
							if (temp.contains("href=\"")) {
								// 截取出商品的url
								goods_url = temp.split("href=\"")[1].split("\"")[0];

								if ((goods_url.startsWith("http://") || goods_url.startsWith("https://"))
										&& baohan(goods_url, keyword)) {
									System.out.println("************************************************************");
									System.out.println("*                                                          *");
									System.out.println("          the goods is number: " + good_amount);
									System.out.println("*                                                          *");
									System.out.println("************************************************************");
									System.out.println("");
									System.out.println("the goods url is " + goods_url);
									System.out.println("\n\n");
									// 存放商品图片的url
									String img_url = e.select("img[src]").first().toString();
									System.out.println("the goods img_url is " + img_url);
									System.out.println("\n\n");
									// 分割出img_url
									img_url = img_url.split("src=\"")[1].split("\"")[0];

									// 延时设置
									// java.util.concurrent.TimeUnit.SECONDS.sleep((int)
									// (Math.random() * 3 + 2));

									// 跳转到商品详情页面
									goods_driver.navigate().to(goods_url);
									// 获取商品详情页面的源代码
									String goodssrc = goods_driver.getPageSource();
									// 将源代码存储为Document类型
									Document doc_goods = Jsoup.parse(goodssrc);

									// goods_map.clear();
									// 获取商品详情，并存放到Map中
									goods_map = getDetail(map, doc_goods, keyword);
									// 向map中的src键值，赋值
									goods_map.put("src", goodssrc);
									// 获取商品的状况（新旧商品）
									if (goods_map.get("condition") != null) {
										if (goods_map.get("condition").contains("new"))
											goods_map.put("condition", "used");
										else {
											goods_map.put("condition", "new");
										}
									} else {
										goods_map.put("condition", "new");
									}
									// 将商品的标题添加到商品的详情中去
									goods_map.put("detail", goods_map.get("detail") + goods_map.get("title"));
									// detail = detail.toUpperCase();
									// 为map中的url键值赋值
									goods_map.put("url", goods_url);
									// 为map中的img_url键值赋值
									goods_map.put("img_url", img_url);

									String s1 = "";
									String s2 = "";
									// 商品Map的迭代器
									Iterator<Map.Entry<String, String>> entries = goods_map.entrySet().iterator();
									// 遍历商品Map
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
									// 将商品详情中的"\"替换为"\\"防止插入数据的时候出错
									s2 = s2.replaceAll("\\\\", "\\\\\\\\");
									// 插入数据
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
										setERRORStatus(keyword);
										e2.printStackTrace();
									}
									// 商品数量自加
									good_amount++;
								}
							}
						}
						// 退出商品页面的FireFox
						goods_driver.quit();
						if (driver.findElement(By.id("pagnNextString")).isEnabled())
							driver.findElement(By.id("pagnNextString")).click();
					}
					System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
					System.out.println("^                                                          ^");
					System.out.println("         the keyword: " + keyword + " has finished              ");
					System.out.println("^                                                          ^");
					System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
					// 退出浏览器
					driver.quit();
				}
				// 将关键词的状态更新为"Y"
				pstmt.execute("UPDATE keyword SET state='Y' WHERE key_word = '" + keyword + "'");
				// classification();
				// 获取关键词用于判断是否有新的关键词需要抓取
				keyword = getKeyword();
			}
			// 关闭数据库连接
			DBUtil.closeConn(rs, pstmt, conn);
			System.out.println("\n\n\n");
			System.out.println("$$$$$      WARNING:There is nothing to crawl!!!      $$$$$");
			System.out.println("\n\n\n");
			System.out.println("--------------------------END--------------------------");
		} catch (Exception e) {
			e.printStackTrace();
			// 关闭数据库连接
			DBUtil.closeConn(rs, pstmt, conn);
		}

	}

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

	/**
	 * 检查页面中的某个元素是否存在
	 * 
	 * @return boolean
	 */
	// public static boolean isElementExist(WebDriver driver, String str) {
	// try {
	// driver.findElement(By.id(str));
	// return true;
	// } catch (Exception e) {
	// e.printStackTrace();
	// return false;
	// }
	// }

	// 获取搜索关键词
	/*
	 * public static ArrayList<String> getKeyword() { ArrayList<String>
	 * keywordlist = new ArrayList<String>(); ResultSet rs = null;
	 * PreparedStatement pstmt = null; Connection conn = null; // 连接test数据库 conn
	 * = DBUtil.getConn("info_rob"); String sql =
	 * "select * from keyword where state='Y'"; try { pstmt =
	 * conn.prepareStatement(""); rs = pstmt.executeQuery(sql); while
	 * (rs.next()) { keywordlist.add(rs.getString(1)); } } catch (SQLException
	 * e) { // TODO Auto-generated catch block e.printStackTrace(); }
	 * DBUtil.closeConn(rs, pstmt, conn); return keywordlist; }
	 */

	// 获取搜索关键词 version2，只获取1个关键词
	public static String getKeyword() {
		String keyword = "";
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		Connection conn = null;
		// 连接test数据库
		conn = DBUtil.getConn("info_rob");
		String sql = "select * from keyword where isnull(state) or state='' or state='E'";
		try {
			pstmt = conn.prepareStatement("");
			rs = pstmt.executeQuery(sql);
			// 从数据库中获取一个搜索的关键词
			if (rs.next()) {
				keyword = rs.getString(1);
			}
			// 更新搜索关键词的状态，方便后续的判断逻辑
			if (!keyword.equals("")) {
				sql = "UPDATE keyword SET state='R' WHERE key_word = '" + keyword + "'";
				pstmt.execute(sql);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// 关闭数据库连接
		DBUtil.closeConn(rs, pstmt, conn);
		return keyword;
	}

	// 获取搜索结果页面的html代码
	public static String getSourcecode(String searchurl) {
		String sourcecode = "";
		// WebDriver driver = new ChromeDriver();
		WebDriver driver = new FirefoxDriver();
		driver.get(searchurl);
		sourcecode = driver.getPageSource();
		driver.quit();
		System.out.println("获取搜索页代码成功");
		return sourcecode;
	}

	public static Document getGoodsPageCode(String url) {
		Document goods_doc = null;
		// WebDriver driver = new ChromeDriver();
		WebDriver driver = new FirefoxDriver();
		driver.get(url);
		goods_doc = Jsoup.parse(driver.getPageSource());
		driver.quit();
		System.out.println("获取商品详情页代码成功");
		return goods_doc;
	}

	// public static void classification() {
	// Connection conn = DBUtil.getConn("info_rob");
	// PreparedStatement ps = null;
	// PreparedStatement ps1 = null;
	// ResultSet rs = null;
	// Set<String> set = new HashSet<String>();
	// try {
	// ps = conn.prepareStatement("");
	// ps1 = conn.prepareStatement("");
	// rs = ps.executeQuery("select distinct class from class_attribute");
	// while (rs.next()) {
	// set.add(rs.getString(1));
	// }
	// rs = ps.executeQuery("select class_data,url from goods where
	// isnull(class_name) or class_name like '%(unclassfied)%' or
	// class_name=''");
	// while (rs.next()) {
	// String s = rs.getString("class_data");
	// String url = rs.getString("url");
	// String flag = "";
	// for (String x : set) {
	// if (!x.contains("&")) {
	// while (s.contains("&") && s.contains("›")) {
	// s = s.substring(s.indexOf("›") + 1);
	// }
	// }
	// if (s.contains(x)) {
	// ps1.execute("update goods set class_name='" + x + "' where url='" + url +
	// "'");
	// flag = "Y";
	// break;
	// }
	// }
	// if (!flag.equals("Y")){
	// while (s.contains("›")){
	// s = s.substring(s.indexOf("›") + 1) + "(unclassfied)";
	// }
	// ps1.execute("update goods set class_name='" + s + "' where url='" + url +
	// "'");
	// }
	// }
	// ps1 = null;
	// DBUtil.closeConn(rs, ps, conn);
	// } catch (Exception e) {
	// e.printStackTrace();
	// DBUtil.closeConn(rs, ps, conn);
	// }
	// }
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
						while (sc.contains("&") && sc.contains("›")) {
							// sc = sc.substring(s.indexOf("›") + 1);
							if (s.lastIndexOf("&") > s.lastIndexOf("›"))
								sc = "";
							else
								sc = sc.substring(s.indexOf("›", s.lastIndexOf("&")) + 1);
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
					if (s.contains("›")) {
						s = s.substring(s.lastIndexOf("›") + 1) + "(undefined)";
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
