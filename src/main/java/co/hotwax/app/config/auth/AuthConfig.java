package co.hotwax.app.config.auth;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Jackson POJO class representing authentication configuration.
 * @author grv
 *
 */
public class AuthConfig {

	@JsonProperty("db-config")
	public DBConfig dbConfig;

	public DBConfig getDbConfig() {
		return dbConfig;
	}

	public void setDbConfig(DBConfig dbConfig) {
		this.dbConfig = dbConfig;
	}

}
