package me.natejones.graphdbtest;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SqliteTestHarness implements GraphDbTestHarness {
	private final File path;
	private Connection connection;
	private boolean committed = false;

	public SqliteTestHarness(File path) {
		this.path = path;
	}

	@Override
	public void startup() {
		File db = new File(path, "test_sqlite.db");
		try {
			String schemaSql;
			try (InputStream is =
					getClass().getResourceAsStream("/graphdb_schema.sql");
					Reader reader = new InputStreamReader(is);
					BufferedReader buffer = new BufferedReader(reader)) {
				schemaSql = buffer.lines()
						.collect(Collectors.joining(System.lineSeparator()));
			}
			connection = DriverManager
					.getConnection("jdbc:sqlite:" + db.getAbsolutePath());
			connection.setAutoCommit(false);
			try (Statement stmt = connection.createStatement()) {
				for (String sql : schemaSql.split(";")) {
					sql = sql.trim();
					if (!sql.isEmpty())
						stmt.addBatch(sql);
				}
				stmt.executeBatch();
				connection.commit();
			}
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Object createVertexType(String name) {
		return name;
	}

	@Override
	public void createEdgeType(String name) {
	}

	@Override
	public Object createVertex(String typeName, Map<String, Object> properties) {
		StringBuilder insertSb = new StringBuilder("INSERT INTO Node (label");
		StringBuilder valuesSb = new StringBuilder("VALUES (?");
		List<Object> values = new ArrayList<>();
		for (Map.Entry<String, Object> e : properties.entrySet()) {
			insertSb.append(", ").append(createProperty(typeName, e.getKey(),
					fromValue(e.getValue()), false));
			valuesSb.append(", ?");
			values.add(e.getValue());
		}
		insertSb.append(") ").append(valuesSb).append(")");
		try (PreparedStatement stmt = connection.prepareStatement(
				insertSb.toString(), Statement.RETURN_GENERATED_KEYS)) {
			stmt.setString(1, typeName);
			for (int i = 0; i < values.size(); i++)
				stmt.setObject(i + 2, values.get(i));
			stmt.executeUpdate();
			ResultSet rset = stmt.getGeneratedKeys();
			rset.next();
			return rset.getInt(1);
		}
		catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public Object createEdge(String edgeType, Object from, Object to) {
		try (PreparedStatement stmt = connection.prepareStatement(
				"INSERT INTO Edge (label, out_nid, in_nid) VALUES (?, ?, ?)",
				Statement.RETURN_GENERATED_KEYS)) {
			stmt.setString(1, edgeType);
			stmt.setInt(2, (Integer) from);
			stmt.setInt(3, (Integer) to);
			ResultSet rset = stmt.getGeneratedKeys();
			rset.next();
			return rset.getInt(1);
		}
		catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public String createProperty(Object target, String name, PropType type,
			boolean indexed) {
		String fieldName = fieldName(name, type);
		try {
			if (!isFieldExist("Node", fieldName)) {
				try (Statement stmt = connection.createStatement()) {
					String sqlType = null;
					switch (type) {
						case FLOAT:
							sqlType = "REAL";
							break;
						case INTEGER:
							sqlType = "INTEGER";
							break;
						case STRING:
							sqlType = "TEXT";
							break;
					}
					if (sqlType == null)
						throw new IllegalArgumentException(
								"Property type not supported: " + type);
					stmt.execute(String.format("ALTER TABLE Node ADD COLUMN %s %s",
							fieldName, sqlType));
					if (indexed)
						stmt.execute(
								String.format("CREATE INDEX idx_node_%s ON Node(%s)",
										fieldName, fieldName));
				}
			}
		}
		catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return fieldName;
	}

	@Override
	public Stream<?> getVertices() {
		List<Integer> verts = new LinkedList<>();
		try (Statement stmt = connection.createStatement();
				ResultSet rset = stmt.executeQuery("SELECT nid FROM Node")) {
			while (rset.next())
				verts.add(rset.getInt(1));
		}
		catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return verts.stream();
	}

	@Override
	public Stream<?> findVertices(Map<String, Object> properties) {
		List<Integer> verts = new LinkedList<>();
		StringBuilder sql = new StringBuilder("SELECT nid FROM Node WHERE 1=1");
		List<Object> values = new ArrayList<>();
		for (Map.Entry<String, Object> e : properties.entrySet()) {
			String fieldName = fieldName(e.getKey(), fromValue(e.getValue()));
			if (!isFieldExist("Node", fieldName))
				return Stream.empty();
			values.add(e.getValue());
			sql.append(" AND ").append(fieldName).append("=?");
		}
		try (PreparedStatement stmt =
				connection.prepareStatement(sql.toString())) {
			for (int i = 0; i < values.size(); i++)
				stmt.setObject(i + 1, values.get(i));
			try (ResultSet rset = stmt.executeQuery()) {
				while (rset.next())
					verts.add(rset.getInt(1));
			}
		}
		catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return verts.stream();
	}

	@Override
	public AutoCloseable beginTx() {
		committed = false;
		return () -> {
			if (!committed)
				connection.rollback();
		};
	}

	@Override
	public void commit(AutoCloseable tx) {
		committed = true;
		try {
			connection.commit();
		}
		catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void shutdown() {
		try {
			connection.close();
		}
		catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private PropType fromValue(Object o) {
		if (o instanceof String)
			return PropType.STRING;
		throw new IllegalArgumentException(
				"Unsupported property type: " + o.getClass());
	}

	private String fieldName(String propertyName, PropType type) {
		return "p_" + type + "_" + propertyName;
	}

	private boolean isFieldExist(String tableName, String fieldName) {
		try (Statement stmt = connection.createStatement();
				ResultSet rset =
						stmt.executeQuery("PRAGMA table_info(" + tableName + ")")) {
			while (rset.next()) {
				if (fieldName.equals(rset.getString("name")))
					return true;
			}
		}
		catch (SQLException e) {
		}
		return false;
	}
}
