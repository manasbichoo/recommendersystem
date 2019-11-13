package co.hotwax.app.config.ml;

/**
 * Jackson POJO representing all ML configurations.
 * @author grv
 *
 */
public class MLConfig {

	ClusteringConfig clusterConfig;

	public ClusteringConfig getClusterConfig() {
		return clusterConfig;
	}

	public void setClusterConfig(ClusteringConfig clusterConfig) {
		this.clusterConfig = clusterConfig;
	}

}
