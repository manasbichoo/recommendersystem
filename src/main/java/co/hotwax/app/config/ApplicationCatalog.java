package co.hotwax.app.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;

import co.hotwax.app.config.auth.AuthConfig;
import co.hotwax.app.config.ml.MLConfig;

/**
 * Represents top level configuration class for the whole application.
 * Corresponds to top level element in YAML file.
 * 
 * @author grv
 *
 */
@JsonRootName(value = "applications")
public class ApplicationCatalog {

	@JsonProperty("authentication")
	public AuthConfig authentication;

	@JsonProperty("ml-config")
	public MLConfig mlConfig;

	public AuthConfig getAuthentication() {
		return authentication;
	}

	public void setAuthentication(AuthConfig authentication) {
		this.authentication = authentication;
	}

	public MLConfig getMlConfig() {
		return mlConfig;
	}

	public void setMlConfig(MLConfig mlConfig) {
		this.mlConfig = mlConfig;
	}

}
