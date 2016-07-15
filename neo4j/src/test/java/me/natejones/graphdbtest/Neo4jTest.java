package me.natejones.graphdbtest;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class Neo4jTest {
	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	@Test
	public void test_neo4j_performance() throws Exception {
		GraphDbPerformanceTest test = new GraphDbPerformanceTest();
		test.runHarness(new Neo4jTestHarness(folder.newFolder("test_neo4j")));
	}
}
