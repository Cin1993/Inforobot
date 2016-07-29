package cn.inforobot;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Set;

import cn.inforobot.database.DBUtil;

public class TestClassData {
	public static void main(String[] args) {
		classification();
	}
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
			rs = ps.executeQuery("select class_data,url from goods where isnull(class_name) or class_name like '%(unclassfied)%' or class_name=''");
			while (rs.next()) {
				String s = rs.getString("class_data");
				String url = rs.getString("url");
				String flag = "";
				for (String x : set) {
					if (!x.contains("&")) {
						while (s.contains("&") && s.contains("›")) {
							s = s.substring(s.indexOf("›") + 1);
							System.out.println(s);
						}
					}
					if (s.contains(x)) {
						ps1.execute("update goods set class_name='" + x + "' where url='" + url + "'");
						flag = "Y";
						break;
					}
				}
				if (!flag.equals("Y")){
					while (s.contains("›")){
						s = s.substring(s.indexOf("›") + 1) + "(unclassfied)";
					}
					while (s.startsWith(" ")){
						s = s.replace(" ", "");
					}
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
