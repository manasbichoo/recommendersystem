package co.hotwax.app.config.ml;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Jackson POJO representing K-Means clustering configuration
 * @author grv
 *
 */
public class ClusteringConfig {

	@JsonProperty("product-category-config")
	ProductCategoryConfig productCategoryConfig;

	public ProductCategoryConfig getProductCategoryConfig() {
		return productCategoryConfig;
	}

	public void setProductCategoryConfig(ProductCategoryConfig config) {
		this.productCategoryConfig = config;
	}

}
