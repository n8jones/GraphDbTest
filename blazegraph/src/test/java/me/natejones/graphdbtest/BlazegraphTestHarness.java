package me.natejones.graphdbtest;

import java.io.File;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.bigdata.rdf.sail.BigdataSailRepository;
import com.blazegraph.gremlin.embedded.BasicRepositoryProvider;
import com.blazegraph.gremlin.embedded.BlazeGraphEmbedded;

public class BlazegraphTestHarness implements GraphDbTestHarness {

	private final File path;
	private Graph graph;

	public BlazegraphTestHarness(File path) {
		this.path = path;
	}

	@Override
	public void startup() {
		Properties props = BasicRepositoryProvider
				.getProperties(new File(path, "journalFile").getAbsolutePath());

		BigdataSailRepository repo = BasicRepositoryProvider.open(props);
		graph = BlazeGraphEmbedded.open(repo);
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
		Vertex v = graph.addVertex(typeName);
		for (Map.Entry<String, Object> e : properties.entrySet())
			v.property(e.getKey(), e.getValue());
		return v;
	}

	@Override
	public Object createEdge(String edgeType, Object from, Object to) {
		return ((Vertex) from).addEdge(edgeType, (Vertex) to);
	}

	@Override
	public Object createProperty(Object target, String name, PropType type,
			boolean indexed) {
		// TODO Auto-generated method stub
		return name;
	}

	@Override
	public Stream<?> getVertices() {
		return graph.traversal().V().toStream();
	}

	@Override
	public Stream<?> findVertices(Map<String, Object> properties) {
		GraphTraversal<Vertex, Vertex> g = graph.traversal().V();
		for (Map.Entry<String, Object> e : properties.entrySet())
			g = g.has(e.getKey(), e.getValue());
		return g.toStream();
	}

	@Override
	public AutoCloseable beginTx() {
		return graph.tx();
	}

	@Override
	public void commit(AutoCloseable tx) {
		((Transaction) tx).commit();
	}

	@Override
	public void shutdown() {
		try {
			graph.close();
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
