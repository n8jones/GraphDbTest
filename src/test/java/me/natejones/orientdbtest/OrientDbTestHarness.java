package me.natejones.orientdbtest;

import java.io.File;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientElementType;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class OrientDbTestHarness implements GraphDbTestHarness {
	private final String path;
	private OrientGraph graph;

	public OrientDbTestHarness(File path) {
		this.path = path.toString();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see me.natejones.orientdbtest.GraphDbTestHarness#startup()
	 */
	@Override
	public void startup() {
		graph = new OrientGraph("plocal:" + path);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * me.natejones.orientdbtest.GraphDbTestHarness#createVertexType(java.lang.
	 * String)
	 */
	@Override
	public Object createVertexType(String name) {
		return graph.executeOutsideTx(g -> g.createVertexType(name));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * me.natejones.orientdbtest.GraphDbTestHarness#createEdgeType(java.lang.
	 * String)
	 */
	@Override
	public void createEdgeType(String name) {
		graph.executeOutsideTx(g -> g.createEdgeType(name));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see me.natejones.orientdbtest.GraphDbTestHarness#createVertex(java.lang.
	 * String, java.util.Map)
	 */
	@Override
	public Object createVertex(String typeName, Map<String, Object> properties) {
		OrientVertex v = graph.addVertex("class:" + typeName);
		for (Map.Entry<String, Object> e : properties.entrySet())
			v.setProperty(e.getKey(), e.getValue());
		// graph.commit();
		return v;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * me.natejones.orientdbtest.GraphDbTestHarness#createEdge(java.lang.String,
	 * java.lang.Object, java.lang.Object)
	 */
	@Override
	public Object createEdge(String edgeType, Object from, Object to) {
		Edge e = ((Vertex) from).addEdge(edgeType, (Vertex) to);
		// graph.commit();
		return e;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see me.natejones.orientdbtest.GraphDbTestHarness#getVertices()
	 */
	@Override
	public Stream<?> getVertices() {
		return StreamSupport.stream(graph.getVertices().spliterator(), false);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see me.natejones.orientdbtest.GraphDbTestHarness#shutdown()
	 */
	@Override
	public void shutdown() {
		graph.shutdown();
	}

	@Override
	public AutoCloseable beginTx() {
		return graph::rollback;
	}

	@Override
	public void commit(AutoCloseable tx) {
		graph.commit();
	}

	@Override
	public Stream<?> findVertices(Map<String, Object> properties) {
		GraphQuery q = graph.query();
		for (Map.Entry<String, Object> e : properties.entrySet())
			q = q.has(e.getKey(), e.getValue());
		return StreamSupport.stream(q.vertices().spliterator(), false);
	}

	@Override
	public Object createProperty(Object target, String name, PropType type,
			boolean indexed) {
		return graph.executeOutsideTx(g -> {
			OrientElementType elementType = (OrientElementType) target;
			OType orientType = OType.ANY;
			switch (type) {
				case FLOAT:
					orientType = OType.FLOAT;
					break;
				case INTEGER:
					orientType = OType.INTEGER;
					break;
				case STRING:
					orientType = OType.STRING;
					break;
			}
			OProperty property = elementType.createProperty(name, orientType);
			if (indexed)
				property.createIndex(INDEX_TYPE.NOTUNIQUE);
			return property;
		});
	}
}