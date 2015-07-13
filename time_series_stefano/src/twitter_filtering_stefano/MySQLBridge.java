package twitter_filtering_stefano;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MySQLBridge {
	/**
	 * @uml.property  name="connect"
	 */
	private Connection connect = null;

	public MySQLBridge(String server,String user,String passwd,String db) throws ClassNotFoundException,SQLException{
		// Caricamento dei driver per la connessione al DB
		Class.forName("com.mysql.jdbc.Driver");
		//inizializzo la connessione al DB
		if(passwd.equals("NULL"))
			connect = DriverManager.getConnection("jdbc:mysql://"+server+"/"+db,user,null);
		else
		connect = DriverManager.getConnection("jdbc:mysql://"+server+"/"+db+"?"
					+ "user="+user+"&password="+passwd);
		connect.setAutoCommit(false);
	}
	
	public ResultSet retrieveData(String query) throws SQLException{
		//invio una richiesta al DB
		Statement statement = connect.createStatement();
		//l'oggetto resultSet raccoglie i risultati della query
		ResultSet resultSet = statement.executeQuery(query);
		return resultSet;
	}
	
	public long insertData(String query) throws SQLException{
		//invio una richiesta al DB
		Statement statement = connect.createStatement();
		//eseguo la query di inserimento/modifica/eliminazione
		long id=statement.executeUpdate(query, Statement.RETURN_GENERATED_KEYS);
		statement.close();
		return id;
	}
	
	
	public Connection getConnection(){
		return connect;
	}
	
	public void closeConnection() {
		//chiudo tutti gli oggetti del DB
		try {
			if(connect!=null) {
				connect.commit();
				connect.close();
			}
		} catch (SQLException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
	}
}
