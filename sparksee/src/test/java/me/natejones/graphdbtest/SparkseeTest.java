package me.natejones.graphdbtest;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SparkseeTest {
	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	@Test
	public void test_sparksee_performance() throws Exception {
		GraphDbPerformanceTest test = new GraphDbPerformanceTest();
		test.runHarness(new SparkseeTestHarness(folder.newFolder("test_sparksee")));
	}
}
