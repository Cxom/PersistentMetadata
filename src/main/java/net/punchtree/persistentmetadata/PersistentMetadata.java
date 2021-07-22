package net.punchtree.persistentmetadata;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.World;
import org.bukkit.block.Block;
import org.bukkit.metadata.FixedMetadataValue;

import net.md_5.bungee.api.ChatColor;

/**
 * Persistent Block Metadata API
 * @author Cxom
 *
 */
public class PersistentMetadata {

	static PersistentMetadataPlugin plugin;
	static Connection conn;
	
	// TODO overridden methods to ensure value is valid
	public static boolean setMetadata(Block block, String key, Object value) {
		assert( conn != null );
		
		PreparedStatement stmt;
		try {
			stmt = conn.prepareStatement(
					"REPLACE INTO blocks (world, x, y, z, metadata_key, metadata_value) "
						+ "VALUES (?, ?, ?, ?, ?, ?);");
			stmt.setString(1, block.getWorld().getName());
			stmt.setInt(2, block.getX());
			stmt.setInt(3, block.getY());
			stmt.setInt(4, block.getZ());
			stmt.setString(5, key);
			stmt.setObject(6, value);
			stmt.executeUpdate();
		} catch (SQLException e) {
			Bukkit.broadcastMessage(ChatColor.RED + "Error storing metadata_key `" + key + "` and value `" + value.toString() + "` to database!");
			e.printStackTrace();
			return false;
		}
		
		block.setMetadata(key, new FixedMetadataValue(plugin, value));
		return true;
	}
	
	public static Map<String, Object> getAllMetadata(Block block) {
		Map<String, Object> metadataMap = new HashMap<>();
		try {
			PreparedStatement st = conn.prepareStatement("SELECT * FROM blocks WHERE world = ? AND x = ? AND y = ? AND z = ?");
			st.setString(1, block.getWorld().getName());
			st.setInt(2, block.getX());
			st.setInt(3, block.getY());
			st.setInt(4, block.getZ());
			ResultSet rs = st.executeQuery();
			while (rs.next()) {
				metadataMap.put(rs.getString("metadata_key"), rs.getObject("metadata_value"));
			}
		}
		catch (SQLException e) {
			Bukkit.broadcastMessage(ChatColor.RED + "Error retrieving metadata values for block `" + blockString(block) + "` from database!");
			e.printStackTrace();
		}
		return metadataMap;
	}
	
	public static Map<Block, Map<String, Object>> getMetadataInRadius(Block center, int radius) {
		World world = center.getWorld();
		Map<Block, Map<String, Object>> metadataMap = new HashMap<>();
		try {
			PreparedStatement st = conn.prepareStatement("SELECT * FROM blocks WHERE world = ? AND x >= ? AND x <= ? AND y >= ? AND y <= ? AND z >= ? AND z <= ?");
			st.setString(1, center.getWorld().getName());
			st.setInt(2, center.getX() - radius);
			st.setInt(3, center.getX() + radius);
			st.setInt(4, center.getY() - radius);
			st.setInt(5, center.getY() + radius);
			st.setInt(6, center.getZ() - radius);
			st.setInt(7, center.getZ() + radius);
			ResultSet rs = st.executeQuery();
			while (rs.next()) {
				Block block = new Location(world, rs.getInt("x"), rs.getInt("y"), rs.getInt("z")).getBlock();
				Map<String, Object> blockMap = metadataMap.get(block);
				if (blockMap == null) {
					blockMap = new HashMap<>();
					metadataMap.put(block, blockMap);
				}
				blockMap.put(rs.getString("metadata_key"), rs.getObject("metadata_value"));
			}
		}
		catch (SQLException e) {
			Bukkit.broadcastMessage(ChatColor.RED + "Error retrieving metadata values in radius of " + radius + " around block `" + blockString(center) + "` from database!");
			e.printStackTrace();
		}
		return metadataMap;
	}
	
	public static Object getMetadata(Block block, String key) {
		try {
			PreparedStatement st = conn.prepareStatement("SELECT * FROM blocks WHERE world = ? AND x = ? AND y = ? AND z = ? AND metadata_key = ?");
			st.setString(1, block.getWorld().getName());
			st.setInt(2, block.getX());
			st.setInt(3, block.getY());
			st.setInt(4, block.getZ());
			st.setString(5, key);
			ResultSet rs = st.executeQuery();
			if (!rs.next()) {
				return null;
			}
			return rs.getObject("metadata_value");
		}
		catch (SQLException e) {
			Bukkit.broadcastMessage(ChatColor.RED + "Error retrieving metadata value for metadata_key `" + key + "` from database!");
			e.printStackTrace();
			return null;
		}
	}
	
	public static boolean removeMetadata(Block block, String key) {
		assert( conn != null );
		
		PreparedStatement stmt;
		try {
			stmt = conn.prepareStatement(
					"DELETE FROM blocks WHERE world = ? AND x = ? AND y = ? AND z = ? AND metadata_key = ?");
			stmt.setString(1, block.getWorld().getName());
			stmt.setInt(2, block.getX());
			stmt.setInt(3, block.getY());
			stmt.setInt(4, block.getZ());
			stmt.setString(5, key);
			stmt.executeUpdate();
		} catch (SQLException e) {
			Bukkit.broadcastMessage(ChatColor.RED + "Error deleting metadata_key `" + key + "` from database!");
			e.printStackTrace();
			return false;
		}
		
		block.removeMetadata(key, plugin);
		return true;
	}
	
	private static String blockString(Block block) {
		return String.format("%s:%d,%d,%d", block.getWorld().getName(), block.getX(), block.getY(), block.getZ()); 
	}
	
}
