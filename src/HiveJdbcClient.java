
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
public class HiveJdbcClient {
	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	private static ResultSet rs;
	public static void main(String[] args) throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}

		Connection con = DriverManager.getConnection("jdbc:hive://127.0.0.1:10001/batch2", "", "");
		Statement stmt = con.createStatement();
		String tableName = "partition";
		rs=stmt.executeQuery("select * from " + tableName);
		while(rs.next()){
			System.out.println(rs.getString(2));
			System.out.println(rs.getString(1));
		}

	}
}

