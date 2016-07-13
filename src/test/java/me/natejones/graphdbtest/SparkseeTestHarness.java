package me.natejones.graphdbtest;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.sparsity.sparksee.gdb.AttributeKind;
import com.sparsity.sparksee.gdb.Condition;
import com.sparsity.sparksee.gdb.DataType;
import com.sparsity.sparksee.gdb.Database;
import com.sparsity.sparksee.gdb.Graph;
import com.sparsity.sparksee.gdb.Objects;
import com.sparsity.sparksee.gdb.Session;
import com.sparsity.sparksee.gdb.Sparksee;
import com.sparsity.sparksee.gdb.SparkseeConfig;
import com.sparsity.sparksee.gdb.Value;

public class SparkseeTestHarness implements GraphDbTestHarness {
	private final File path;
	private Sparksee sparksee;
	private Database db;
	private Session session;
	private Graph graph;

	public SparkseeTestHarness(File path) {
		this.path = path;
	}

	@Override
	public void startup() {
		SparkseeConfig cfg = new SparkseeConfig();
		sparksee = new Sparksee(cfg);
		try {
			db = sparksee.create(new File(path, "db.gdb").getAbsolutePath(), "db");
		}
		catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
		session = db.newSession();
		graph = session.getGraph();
	}

	@Override
	public Object createVertexType(String name) {
		return graph.newNodeType(name);
	}

	@Override
	public void createEdgeType(String name) {
		graph.newEdgeType(name, true, true);
	}

	@Override
	public Object createVertex(String typeName, Map<String, Object> properties) {
		int type = graph.findType(typeName);
		long node = graph.newNode(type);
		for (Map.Entry<String, Object> e : properties.entrySet()) {
			int attribute = graph.findAttribute(type, e.getKey());
			// XXX Not allowing arbitrary properties will be a huge limiting factor
			// for our purposes.
			if (attribute < 1)
				attribute = graph.newAttribute(type, e.getKey(), DataType.String,
						AttributeKind.Basic);
			Value value = new Value();
			if (e.getValue() instanceof String)
				value.setString((String) e.getValue());
			graph.setAttribute(node, attribute, value);
		}
		return node;
	}

	@Override
	public Object createEdge(String edgeType, Object from, Object to) {
		int type = graph.findType(edgeType);
		return graph.newEdge(type, (Long) from, (Long) to);
	}

	@Override
	public Object createProperty(Object target, String name, PropType type,
			boolean indexed) {
		Integer targetType = (Integer) target;
		DataType dt;
		switch (type) {
			case FLOAT:
				dt = DataType.Double;
				break;
			case INTEGER:
				dt = DataType.Integer;
				break;
			case STRING:
			default:
				dt = DataType.String;
				break;
		}
		return graph.newAttribute(targetType, name, dt,
				indexed ? AttributeKind.Indexed : AttributeKind.Basic);
	}

	@Override
	public Stream<?> getVertices() {
		return StreamSupport.stream(graph.findNodeTypes().spliterator(), false)
				.flatMap(type -> stream(graph.select(type)));
	}

	@Override
	public Stream<?> findVertices(Map<String, Object> properties) {
		int type = graph.findType(GraphDbPerformanceTest.ARTIFACT_ELEMENT);
		int attribute = graph.findAttribute(type, GraphDbPerformanceTest.ID);
		Value v = new Value();
		v.setString(properties.get(GraphDbPerformanceTest.ID).toString());
		return stream(graph.select(attribute, Condition.Equal, v));
	}

	private Stream<Long> stream(Objects o) {
		try {
			List<Long> ids = new LinkedList<>();
			o.stream().forEach(ids::add);
			return ids.stream();
		}
		finally {
			o.close();
		}
	}

	private boolean committed;

	@Override
	public AutoCloseable beginTx() {
		session.beginUpdate();
		committed = false;
		// Fake transaction
		return () -> {
			if (!committed)
				session.rollback();
		};
	}

	@Override
	public void commit(AutoCloseable tx) {
		session.commit();
		committed = true;
	}

	@Override
	public void shutdown() {
		try {
			session.close();
			db.close();
			sparksee.close();
		}
		catch (Throwable t) {
			t.printStackTrace();
		}
	}

}
