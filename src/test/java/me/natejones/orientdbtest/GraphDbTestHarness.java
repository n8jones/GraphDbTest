package me.natejones.orientdbtest;

import java.util.Map;
import java.util.stream.Stream;

public interface GraphDbTestHarness {

	void startup();

	Object createVertexType(String name);

	void createEdgeType(String name);

	Object createVertex(String typeName, Map<String, Object> properties);

	Object createEdge(String edgeType, Object from, Object to);

	Object createProperty(Object target, String name, PropType type,
			boolean indexed);

	Stream<?> getVertices();

	Stream<?> findVertices(Map<String, Object> properties);

	AutoCloseable beginTx();

	void commit(AutoCloseable tx);

	void shutdown();

	public enum PropType {
		STRING, INTEGER, FLOAT;
	}
}