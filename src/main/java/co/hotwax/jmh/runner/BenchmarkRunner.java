package co.hotwax.jmh.runner;

import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import co.hotwax.ml.clustering.ProductCategoryClustering;

public class BenchmarkRunner {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Options opt = new OptionsBuilder().include(ProductCategoryClustering.class.getSimpleName()).forks(1).build();

		try {
			new Runner(opt).run();
		} catch (RunnerException e) {
			// TODO Auto-generated catch block
			System.out.println("Error running benchmark suite ...");
			e.printStackTrace();
		}

	}

}
