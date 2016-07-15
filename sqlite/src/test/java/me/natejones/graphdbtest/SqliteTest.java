package me.natejones.graphdbtest;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SqliteTest {
	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	@Test
	public void test_sqlite_performance() throws Exception {
		GraphDbPerformanceTest test = new GraphDbPerformanceTest();
		test.runHarness(new SqliteTestHarness(folder.newFolder("test_sqlite")));
	}
}
