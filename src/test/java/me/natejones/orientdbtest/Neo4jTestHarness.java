package me.natejones.orientdbtest;

import java.io.File;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class Neo4jTestHarness implements GraphDbTestHarness {
	private GraphDatabaseService graphDb;
	private final File path;

	public Neo4jTestHarness(File path) {
		this.path = path;
	}

	@Override
	public void startup() {
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(path);
	}

	@Override
	public Object createVertexType(String name) {
		return Label.label(name);
	}

	@Override
	public void createEdgeType(String name) {
	}

	@Override
	public Object createVertex(String typeName, Map<String, Object> properties) {
		Node n = graphDb.createNode(Label.label(typeName));
		for (Map.Entry<String, Object> e : properties.entrySet())
			n.setProperty(e.getKey(), e.getValue());
		return n;

	}

	@Override
	public Object createEdge(String edgeType, Object from, Object to) {
		Relationship r = ((Node) from).createRelationshipTo((Node) to,
				RelationshipType.withName(edgeType));
		return r;
	}

	@Override
	public Stream<?> getVertices() {
		Transaction tx = graphDb.beginTx();
		return graphDb.getAllNodes().stream().onClose(tx::close);
	}

	@Override
	public void shutdown() {
		graphDb.shutdown();
	}

	@Override
	public AutoCloseable beginTx() {
		return graphDb.beginTx();
	}

	@Override
	public void commit(AutoCloseable tx) {
		((Transaction) tx).success();
	}

	@Override
	public Stream<?> findVertices(Map<String, Object> properties) {
		StringBuilder sb = new StringBuilder("MATCH (n) WHERE ");
		boolean first = true;
		for (Map.Entry<String, Object> e : properties.entrySet()) {
			if (first)
				first = false;
			else
				sb.append(" AND ");
			sb.append("n.").append(e.getKey()).append(" = {").append(e.getKey())
					.append("}");
		}
		sb.append(" RETURN n");
		return graphDb.execute(sb.toString(), properties).<Node> columnAs("n")
				.stream();
	}

	@Override
	public Object createProperty(Object target, String name, PropType type,
			boolean indexed) {
		try (Transaction tx = graphDb.beginTx()) {
			Label label = (Label) target;
			if (indexed)
				graphDb.schema().indexFor(label).on(name).create();
			tx.success();
			return name;
		}
	}
}