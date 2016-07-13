package me.natejones.orientdbtest;

import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;

public class Main {

	public static void main(String[] args) {
		System.out.println("Hello world");
		OrientGraph g = new OrientGraph("remote:localhost/Notes", "root", "root");
		try {
			for (Vertex v : g.getVertices()) {
				System.out.println(v.getId());
				System.out.println(v.getClass());
				for (String key : v.getPropertyKeys())
					System.out.printf("\t%s : %s%n", key, v.getProperty(key));
			}
		}
		finally {
			g.shutdown();
		}
	}
}
