package mqtt_test01;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * 数据库连接对象。
 *
 */
public class DBConn {
	//public static final String host = "jdbc://mysql://172.16.7.15:3306/mqtt";	// java.sql.SQLException: No suitable driver found for jdbc://mysql://172.16.7.15:3306/mqtt
	public static final String host = "jdbc:mysql://172.16.7.15:3306/mqtt?serverTimezone=UTC&characterEncoding=utf-8";
	public static final String driver = "com.mysql.jdbc.Driver";
	public static final String user = "root";
	public static final String password = "admin123***";
	
	public Connection conn = null;
	public PreparedStatement pst = null;
	
	
	public DBConn() {
		try {
			Class.forName(driver);
			conn = DriverManager.getConnection(host, user, password);
			

			Date date = new Date();
			//DateFormat df = DateFormat.getDateTimeInstance();
	        SimpleDateFormat df = new SimpleDateFormat("[yyyy-MM-dd HH:mm:ss]  ");//设置日期格式
			System.out.println(df.format(date) + "数据库连接成功（DB） ...");
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void executeSql(String sql) {
		try {
			pst = conn.prepareStatement(sql);
			Boolean res = pst.execute(sql);	// Can not issue data manipulation statements with executeQuery(). （如果是insert、update的话，用execute）
			if(res) {
				System.out.println("Execute SQL Success.");
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void closeDB() {
		try {
			this.conn.close();
			this.pst.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
