package me.natejones.orientdbtest;

import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import me.natejones.orientdbtest.GraphDbTestHarness.PropType;

public class GraphDbPerformanceTest {
	private static final String PROJECT = "Project";
	private static final String PROJECT_HAS_ARTIFACT = "project_has_artifact";
	private static final String ARTIFACT = "Artifact";
	private static final String ARTIFACT_HAS_ARTIFACT_VERSION =
			"artifact_has_artifact_version";
	private static final String ARTIFACT_VERSION = "ArtifactVersion";
	private static final String ARTIFACT_VERSION_HAS_ARTIFACT_ELEMENT =
			"artifact_version_has_artifact_element";
	private static final String ARTIFACT_ELEMENT = "ArtifactElement";
	private static final String ID = "aid";
	private static int VERTEX_COUNT = 10_000;

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	@Test
	public void test_orient_db() throws Exception {
		runHarness(new OrientDbTestHarness(folder.newFolder("test_orient_db")));
	}

	@Test
	public void test_neo4j() throws Exception {
		runHarness(new Neo4jTestHarness(folder.newFolder("test_neo4j")));
	}

	public void runHarness(final GraphDbTestHarness harness) throws Exception {
		System.out
				.println(harness.getClass().getSimpleName() + " x" + VERTEX_COUNT);
		LocalTime start = LocalTime.now();
		time("Startup", harness::startup);
		try {
			time("SetupSchema", () -> setupSchema(harness));
			time("InsertData", () -> insertData(harness));
			time("Count Stream", () -> assertEquals("Count equals",
					VERTEX_COUNT + 3, harness.getVertices().count()));
			time("FindEach", () -> findEach(harness));
		}
		finally {
			time("Shutdown", harness::shutdown);
			System.out
					.println("Total: " + Duration.between(start, LocalTime.now()));
		}
	}

	private void setupSchema(GraphDbTestHarness harness) {
		harness.createVertexType(PROJECT);
		harness.createEdgeType(PROJECT_HAS_ARTIFACT);
		harness.createVertexType(ARTIFACT);
		harness.createEdgeType(ARTIFACT_HAS_ARTIFACT_VERSION);
		harness.createVertexType(ARTIFACT_VERSION);
		harness.createEdgeType(ARTIFACT_VERSION_HAS_ARTIFACT_ELEMENT);
		Object artifactElement = harness.createVertexType(ARTIFACT_ELEMENT);
		harness.createProperty(artifactElement, ID, PropType.STRING, true);
	}

	private void insertData(GraphDbTestHarness harness) {
		try (AutoCloseable tx = harness.beginTx()) {
			Object project = harness.createVertex(PROJECT, map(ID, "TEST"));
			Object artifact = harness.createVertex(ARTIFACT, map(ID, "ART1"));
			harness.createEdge(PROJECT_HAS_ARTIFACT, project, artifact);
			Object version = harness.createVertex(ARTIFACT_VERSION, map(ID, "1"));
			harness.createEdge(ARTIFACT_HAS_ARTIFACT_VERSION, artifact, version);
			for (int i = 0; i < VERTEX_COUNT; i++) {
				Object element =
						harness.createVertex(ARTIFACT_ELEMENT, map(ID, "TEST-" + i));
				harness.createEdge(ARTIFACT_VERSION_HAS_ARTIFACT_ELEMENT, version,
						element);
			}
			harness.commit(tx);
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void findEach(GraphDbTestHarness harness) {
		try (AutoCloseable tx = harness.beginTx()) {
			for (int i = 0; i < VERTEX_COUNT; i++) {
				String id = "TEST-" + i;
				assertEquals("Find " + id, 1,
						harness.findVertices(map(ID, id)).count());
			}
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private Map<String, Object> map(Object... keysAndValues) {
		Map<String, Object> map = new HashMap<String, Object>();
		for (int i = 0; i < keysAndValues.length;)
			map.put(keysAndValues[i++].toString(), keysAndValues[i++]);
		return map;
	}

	void time(String name, Runnable r) {
		LocalTime start = LocalTime.now();
		try {
			r.run();
		}
		finally {
			System.out.println(
					"\t" + name + ":\t" + Duration.between(start, LocalTime.now()));
		}
	}
}
