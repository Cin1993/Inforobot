package cn.inforobot;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import cn.inforobot.database.DBUtil;

public class AutoCreateTableTest {
	public static void main(String[] args) {
		String classname = "laptop";
		creationtable(getcolumn(classname), classname);

	}
	public static Map<String, String> getcolumn(String classname) {
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		Connection conn = null;
		// 连接test数据库
		conn = DBUtil.getConn("test");

		String sql = "select * from class_detail where class_name = '"+ classname +"' or class_name = 'others'";
		Map<String, String> map = new HashMap<String, String>();
		try {
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				map.put(rs.getString(3), rs.getString(6));
			}
			DBUtil.closeConn(rs, pstmt, conn);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return map;
	}
	
	public static Boolean creationtable(Map<String, String> map, String classname){
		String sql = "create table if not exists " + classname +"(pid int(10) AUTO_INCREMENT "
				+ "PRIMARY KEY, ";
		Iterator<Map.Entry<String, String>> entries = map.entrySet().iterator();
		while (entries.hasNext()) {
			Map.Entry<String, String> entry = entries.next();
			if (entries.hasNext()) {
				sql += "`" + entry.getKey() + "` " + entry.getValue() + ","; 
			} else {
				sql += "`" + entry.getKey() + "` " + entry.getValue() + ")"; 
			}
		}
		ResultSet rs = null;
		PreparedStatement pstmt = null;
		Connection conn = null;
		// 连接test数据库
		conn = DBUtil.getConn("test");
		try {
			pstmt = conn.prepareStatement(sql);
			pstmt.execute();
			DBUtil.closeConn(rs, pstmt, conn);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}
	
}
