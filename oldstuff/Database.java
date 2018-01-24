import java.sql.*;

public class Database {
	
	private String connectionUrl = "jdbc:mysql://localhost/weatherdata";
	private String dbUser = "root";
	private String dbPwd = "";
	private Connection conn;
	private ResultSet rs;
	
	static {
		try {
			Class.forName("com.mysql.jbdc.Driver");
		} catch (ClassNotFoundException cnf) {
			System.out.println("Driver could not be loaded: " + cnf);
		}
	}
	
	public synchronized void insert( String query ) {		
		try {
			conn = DriverManager.getConnection(connectionUrl, dbUser, dbPwd);
			Statement stmt = conn.createStatement();
			
			stmt.executeUpdate(query);
			
			if(conn != null) {
				conn.close();
				conn = null;
			}
		} catch (SQLException sqle) {
			System.out.println("SQL Exception thrown: " + sqle);
		}
	}

}
