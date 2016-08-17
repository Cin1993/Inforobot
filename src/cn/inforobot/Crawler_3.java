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
		// System.setProperty("webdriver.chrome.driver",
		// "C:\\chromedriver.exe");
		// 鑾峰彇鎼滅储缃戠珯鐨凩ist
		int round = 0;
		while (true) {
			List<Host_configure> host_configure = dao.getHostConfigure();
			ResultSet rs = null;
			PreparedStatement pstmt = null;
			Connection conn = null;
			// 杩炴帴info_rob鏁版嵁搴�
			conn = DBUtil.getConn("info_rob");

			String sql = "select * from class_detail where crawl_depth = '2'";
			// String keyword = "";
			// 瀛樻斁class_details琛ㄦ牸3/5鍒楃殑鍐呭顔�
			Map<String, String> map = new HashMap<String, String>();
			try {
				pstmt = conn.prepareStatement(sql);
				rs = pstmt.executeQuery();
				while (rs.next()) {
					map.put(rs.getString(3), rs.getString(5));
				}
				// 鑾峰彇棣栨鎶撳彇鐨勫叧閿瘝
				String keyword = getKeyword();
				// 鎼滅储鍏抽敭璇嶇殑寰幆
				while (!keyword.equals("")) {
					// 鎼滅储缃戠珯鐨勫惊鐜�
					for (int i = 0; i < host_configure.size(); i++) {
						// WebDriver driver = new ChromeDriver();
						// 鍚姩FireFox
						WebDriver driver = new FirefoxDriver();
						// 瀛楃涓叉嫾鍑烘煡璇㈠晢鍝佺殑url
						String url = host_configure.get(i).getSearch_box_url() + keyword;
						// 璺宠浆鍒板晢鍝佺殑url
						driver.get(url);
						// 浣跨敤Selenium鑾峰彇缃戦〉鐨勬簮浠ｇ爜锛屽苟瀛樺偍涓篔soup鐨凞ocument绫诲瀷
						Document doc0 = Jsoup.parse(driver.getPageSource());
						// 閫氳繃Jsoup鐨勯�夋嫨鍣紝閫夋嫨鍑虹炕椤电殑鏍囩
						Element nextpage = doc0.select("a[id=pagnNextLink]").first();
						// 缁熻鎶撳彇鍒扮殑鍟嗗搧鐨勬暟閲�
						int good_amount = 1;
						// 缈婚〉
						while (!(nextpage == null || nextpage.toString().equals(""))) {
							// 璺宠浆鍒板晢鍝佺殑椤甸潰
							driver.get(url);
							// 鑾峰彇鍟嗗搧椤甸潰鐨勬簮浠ｇ爜
							Document doc = Jsoup.parse(driver.getPageSource());
							// 绉婚櫎椤甸潰鐨刯s浠ｇ爜
							// doc.select("script").remove();
							// 閫氳繃select閫夋嫨鍣紝閫夋嫨鍑烘煡璇㈠埌鍟嗗搧鐨勫垪琛�
							Elements es = doc.select("ul[id=s-results-list-atf]>li");
							// 鑾峰彇缈婚〉鏍囪瘑
							nextpage = doc.select("a[id=pagnNextLink]").first();
							// 鍒ゆ柇缈婚〉鏍囪瘑鏄惁涓虹┖锛屼互纭畾鏄惁鍒拌揪鏈�鍚庝竴椤�
							if (!(nextpage == null || nextpage.toString().equals(""))) {
								// 鎴彇缈婚〉鍚庣殑椤甸潰鐨剈rl
								url = nextpage.toString().split("href=\"")[1].split("\"")[0];
								// 瀛楃涓叉嫾鍑戝嚭缈婚〉鍚庣殑url
								url = "http://www.amazon.com" + url;
							}
							// 瀛樻斁鍟嗗搧鐨剈rl鐨勮凯浠ｅ櫒
							Iterator<Element> listIterator = es.iterator();
							// 涓存椂瀛楃涓�
							String temp = "";
							// 瀛樻斁鍟嗗搧椤甸潰鐨剈rl
							String goods_url = "";
							// WebDriver goods_driver = new ChromeDriver();
							// 寮�鍚疐ireFox鎵撳紑鍟嗗搧椤甸潰
							WebDriver goods_driver = new FirefoxDriver();
							// 寰幆鑾峰彇杩唬鍣ㄤ腑鐨勫晢鍝�
							while (listIterator.hasNext()) {
								// 鐢ㄤ簬鑾峰彇鍟嗗搧鐨刬mg_url鍜寀rl鐨勮凯浠ｅ櫒
								Element e = listIterator.next();
								// 瀛樻斁鍟嗗搧淇℃伅鐨凪ap
								Map<String, String> goods_map = new HashMap<String, String>();
								temp = e.getElementsByAttribute("href").toString();
								if (temp.contains("href=\"")) {
									// 鎴彇鍑哄晢鍝佺殑url
									goods_url = temp.split("href=\"")[1].split("\"")[0];

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
										// 瀛樻斁鍟嗗搧鍥剧墖鐨剈rl
										String img_url = e.select("img[src]").first().toString();
										System.out.println("the goods img_url is " + img_url);
										System.out.println("\n\n");
										// 鍒嗗壊鍑篿mg_url
										img_url = img_url.split("src=\"")[1].split("\"")[0];

										// 寤舵椂璁剧疆
										// java.util.concurrent.TimeUnit.SECONDS.sleep((int)
										// (Math.random() * 3 + 2));

										// 璺宠浆鍒板晢鍝佽鎯呴〉闈�
										goods_driver.navigate().to(goods_url);
										// 鑾峰彇鍟嗗搧璇︽儏椤甸潰鐨勬簮浠ｇ爜
										String goodssrc = goods_driver.getPageSource();
										// 灏嗘簮浠ｇ爜瀛樺偍涓篋ocument绫诲瀷
										Document doc_goods = Jsoup.parse(goodssrc);

										// goods_map.clear();
										// 鑾峰彇鍟嗗搧璇︽儏锛屽苟瀛樻斁鍒癕ap涓�
										goods_map = getDetail(map, doc_goods, keyword);
										// 鍚憁ap涓殑src閿�硷紝璧嬪��
										goods_map.put("src", goodssrc);
										// 鑾峰彇鍟嗗搧鐨勭姸鍐碉紙鏂版棫鍟嗗搧锛�
										if (goods_map.get("condition") != null) {
											if (goods_map.get("condition").contains("new"))
												goods_map.put("condition", "used");
											else {
												goods_map.put("condition", "new");
											}
										} else {
											goods_map.put("condition", "new");
										}
										// 灏嗗晢鍝佺殑鏍囬娣诲姞鍒板晢鍝佺殑璇︽儏涓幓
										goods_map.put("detail", goods_map.get("detail") + goods_map.get("title"));
										// detail = detail.toUpperCase();
										// 涓簃ap涓殑url閿�艰祴鍊�
										goods_map.put("url", goods_url);
										// 涓簃ap涓殑img_url閿�艰祴鍊�
										goods_map.put("img_url", img_url);

										String s1 = "";
										String s2 = "";
										// 鍟嗗搧Map鐨勮凯浠ｅ櫒
										Iterator<Map.Entry<String, String>> entries = goods_map.entrySet().iterator();
										// 閬嶅巻鍟嗗搧Map
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
										// 灏嗗晢鍝佽鎯呬腑鐨�"\"鏇挎崲涓�"\\"闃叉鎻掑叆鏁版嵁鐨勬椂鍊欏嚭閿�
										s2 = s2.replaceAll("\\\\", "\\\\\\\\");
										// 鎻掑叆鏁版嵁
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
										// 鍟嗗搧鏁伴噺鑷姞
										good_amount++;
									}
								}
							}
							// 閫�鍑哄晢鍝侀〉闈㈢殑FireFox
							goods_driver.quit();
							if (driver.findElement(By.id("pagnNextString")).isEnabled())
								driver.findElement(By.id("pagnNextString")).click();
						}
						System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
						System.out.println("^                                                          ^");
						System.out.println("         the keyword: " + keyword + " has finished              ");
						System.out.println("^                                                          ^");
						System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
						// 閫�鍑烘祻瑙堝櫒
						driver.quit();
					}
					// 灏嗗叧閿瘝鐨勭姸鎬佹洿鏂颁负"Y"
					pstmt.execute("UPDATE keyword SET state='Y' WHERE key_word = '" + keyword + "'");
					// classification();
					// 鑾峰彇鍏抽敭璇嶇敤浜庡垽鏂槸鍚︽湁鏂扮殑鍏抽敭璇嶉渶瑕佹姄鍙�
					keyword = getKeyword();
				}
				// 鍏抽棴鏁版嵁搴撹繛鎺�
				DBUtil.closeConn(rs, pstmt, conn);
				System.out.println("\n\n\n");
				System.out.println("$$$$$      WARNING:There is nothing to crawl!!!      $$$$$");
				System.out.println("\n\n\n");
			} catch (Exception e) {
				e.printStackTrace();
				// 鍏抽棴鏁版嵁搴撹繛鎺�
				DBUtil.closeConn(rs, pstmt, conn);
			}
			System.out.println("\n\n\n");
			System.out.println("$$$$$      MESSAGE:Starting crawling price again      $$$$$");
			System.out.println("\n\n\n");
			try {
				getCartPrice(noPrice());
				getBuyboxPrice(noPrice());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("*********         round:" + round + "        **********");
			System.out.println("--------------------------END--------------------------");
			System.out.println("\n\n\n\n\n");
			try {
				java.util.concurrent.TimeUnit.SECONDS.sleep(120);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static void getCartPrice(ArrayList<String> noprice_list) throws Exception {
		WebDriver driver = new FirefoxDriver();
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		Connection conn = null;
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

	// 鑾峰彇娌℃湁浠锋牸鐨勫晢鍝佺殑url锛岃繘琛屽娆″尮閰�
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

	// 灏嗗叧閿瘝鍑洪敊鐨勭姸鎬佺疆涓�"E"
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

	// 澶勭悊鍏抽敭璇嶄腑鍖呭惈20%鐨勭壒娈婃儏鍐�
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

	// 鑾峰彇鎼滅储鍏抽敭璇� version2锛屽彧鑾峰彇1涓叧閿瘝
	public static String getKeyword() {
		String keyword = "";
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		Connection conn = null;
		// 杩炴帴test鏁版嵁搴�
		conn = DBUtil.getConn("info_rob");
		String sql = "select * from keyword where isnull(state) or state='' or state='E' or (state='R' and robot_name='"
				+ getComputerName() + "')";
		try {
			pstmt = conn.prepareStatement("");
			rs = pstmt.executeQuery(sql);
			// 浠庢暟鎹簱涓幏鍙栦竴涓悳绱㈢殑鍏抽敭璇�
			if (rs.next()) {
				keyword = rs.getString(1);
			}
			// 鏇存柊鎼滅储鍏抽敭璇嶇殑鐘舵�侊紝鏂逛究鍚庣画鐨勫垽鏂�昏緫
			if (!keyword.equals("")) {
				sql = "UPDATE keyword SET state='R',robot_name='" + getComputerName() + "' WHERE key_word = '" + keyword
						+ "'";
				pstmt.execute(sql);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// 鍏抽棴鏁版嵁搴撹繛鎺�
		DBUtil.closeConn(rs, pstmt, conn);
		return keyword;
	}

	public static boolean IsElementPresent(WebDriver driver, By by) {
		try {
			driver.findElement(by);
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	public static String getComputerName() {
		String computerName = System.getenv().get("COMPUTERNAME");
		return computerName;
	}

	// 鍟嗗搧鍒嗙被浠ｇ爜
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
