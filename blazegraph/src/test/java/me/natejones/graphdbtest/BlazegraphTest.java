package me.natejones.graphdbtest;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class BlazegraphTest {
	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	@Test
	public void test_blazegraph_performance() throws Exception {
		GraphDbPerformanceTest test = new GraphDbPerformanceTest();
		test.runHarness(new BlazegraphTestHarness(folder.newFolder("test_neo4j")));
	}
}
