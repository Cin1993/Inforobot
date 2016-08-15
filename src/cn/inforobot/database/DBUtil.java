package cn.inforobot.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

//Program Purpose: Connect to database Class
//Program input: N/A
//Program output: N/A
//Program data/time: 2015/7/1 13:29
//Create by: Jack
//Create time: 2015/7/1
//Program start
public class DBUtil {
    
//    private final static String DRIVER_CLASS="com.mysql.jdbc.Driver";//数据库驱动名
//    private final static String CONN_STR="jdbc:mysql://localhost:3306";//数据库连接名
//    private final static String DB_USER="root";//数据库用户名
//    private final static String DB_PWD="zx980635254";//数据库密码
    
//    private final static String DRIVER_CLASS="com.mysql.jdbc.Driver";//数据库驱动名
//    private final static String CONN_STR="jdbc:mysql://localhost:3306";//数据库连接名
//    private final static String DB_USER="root";//数据库用户名
//    private final static String DB_PWD="RobotGo!";//数据库密码
    
    
	private final static String DRIVER_CLASS="com.mysql.jdbc.Driver";//数据库驱动名
    private final static String CONN_STR="jdbc:mysql://107.180.75.193:3306";//数据库连接名
    private final static String DB_USER="root";//数据库用户名
    private final static String DB_PWD="USAGo!";//数据库密码
    
    /*private final static String DRIVER_CLASS="com.mysql.jdbc.Driver";//数据库驱动名
    private final static String CONN_STR="jdbc:mysql://121.41.55.91:3306";//数据库连接名
    private final static String DB_USER="root";//数据库用户名
    private final static String DB_PWD="ApacheGo!";//数据库密码    
*/	
    /*private final static String DRIVER_CLASS="com.mysql.jdbc.Driver";//数据库驱动名
    private final static String CONN_STR="jdbc:mysql://127.0.0.1:3306";//数据库连接名
    private final static String DB_USER="root";//数据库用户名
    private final static String DB_PWD="Gwave!";//数据库密码
*/	static{
        try{
            Class.forName(DRIVER_CLASS);
        }catch(ClassNotFoundException e){
        	System.out.println("Sorry,can`t find the Driver!");
            e.printStackTrace();
        }
    }
    
    // Program Purpose: Connect to database function
 	// Program input: N/A
 	// Program output: Connection
 	// Program data/time: 2015/7/1 13:32
 	// Create by: Jack
 	// Create time: 2015/7/1
 	// Program start
    
    
    //连接数据库的方法，传入参数为数据库的库名
    public static Connection getConn(String database){
        try {
        	String connstr = CONN_STR;
        	if(!database.equals(""))
        		connstr = connstr+"/"+database;
            return DriverManager.getConnection(connstr,DB_USER,DB_PWD);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
    //Program end
    
    // Program Purpose: Close connect to database function
  	// Program input: ResultSet,PreparedStatement,Connection
  	// Program output: N/A
  	// Program data/time: 2015/7/1 13:32
  	// Create by: Jack
  	// Create time: 2015/7/1
  	// Program start
    
    //关闭数据库的方法
    public static void closeConn(ResultSet rs,PreparedStatement pstmt,Connection conn){
        try {
            if (rs!=null) {//���صĽ�������Ϊ��,�͹ر�����
                rs.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            if (pstmt!=null) {
                pstmt.close();//�ر�Ԥ�������
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        try {
            if (conn!=null) {
                conn.close();//�رս�����
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    //动态获取数据库的连接，用在同步数据上，传入参数为数据库名，数据库连接名，用户名和密码
    public static Connection getDynamicConn(String database,String conn_str,String bd_user,String bd_pwd){
        try {
        	String connstr = conn_str;
        	if(!database.equals(""))
        		connstr = connstr+"/"+database;
            return DriverManager.getConnection(connstr,bd_user,bd_pwd);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null; 
    }
}
