package net.punchtree.persistentmetadata;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.World;
import org.bukkit.block.Block;
import org.bukkit.metadata.FixedMetadataValue;
import org.bukkit.plugin.java.JavaPlugin;

import net.md_5.bungee.api.ChatColor;

public class PersistentMetadataPlugin extends JavaPlugin {

	private static PersistentMetadataPlugin instance;
	static PersistentMetadataPlugin getInstance() {
		return instance;
	}
	
	private static Connection conn;
	static Connection getConnection() {
		return conn;
	}
	
	@Override
	public void onEnable() {
		instance = this;
		PersistentMetadata.plugin = this;
		
		establishDatabase();
		restoreMetadata();
	}
	
	private void establishDatabase() {
		getDataFolder().mkdir();
		
		try {
			conn = DriverManager.getConnection("jdbc:sqlite:plugins/PersistentMetadata/persistentmetadata.db");
			conn.createStatement().executeUpdate(
					"CREATE TABLE IF NOT EXISTS blocks ("
					+ "rowid INTEGER PRIMARY KEY,"
					+ "world TEXT,"
					+ "x INTEGER,"
					+ "y INTEGER,"
					+ "z INTEGER,"
					+ "metadata_key TEXT,"
					+ "metadata_value)" /* VARIANT */ );
			PersistentMetadata.conn = conn;
		} catch ( SQLException e ) {
			System.out.println("Could not initialize database! " + e.getMessage());
			e.printStackTrace();
			Bukkit.getPluginManager().disablePlugin(this);
		}
	}
	
	// TODO thread this for scaling
	private void restoreMetadata() {
		try {
			// We're going through the whole table
			// USE A CURSOR!
			Statement st = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
			st.setFetchSize(100);
			ResultSet rs = st.executeQuery("SELECT * FROM blocks");
			while ( rs.next() ) {
				World world = Bukkit.getWorld(rs.getString("world"));
				Block block = new Location(world, rs.getInt("x"), rs.getInt("y"), rs.getInt("z")).getBlock();
				block.setMetadata(rs.getString("metadata_key"), new FixedMetadataValue(instance, rs.getObject("metadata_value")));
			}
		} catch (SQLException e) {
			Bukkit.broadcastMessage(ChatColor.DARK_RED + "ERROR: " + ChatColor.RED + "Error while restoring block metadata - aborting. A stacktrace has been printed to console.");
			e.printStackTrace();
		}
	}
	
}
