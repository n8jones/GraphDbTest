package me.natejones.graphdbtest;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class OrientDbTest {
	@Rule
	public TemporaryFolder folder = new TemporaryFolder();
	
	@Test
	public void test_orient_db_performance() throws Exception {
		GraphDbPerformanceTest test = new GraphDbPerformanceTest();
		test.runHarness(
				new OrientDbTestHarness(folder.newFolder("test_orient_db")));
	}
}
