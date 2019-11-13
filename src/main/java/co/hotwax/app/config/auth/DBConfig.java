package co.hotwax.app.config.auth;

import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * Jackson POJO representing DB configuration for the application.
 * @author grv
 *
 */
public class DBConfig {

	@JsonProperty("name")
	private String name;

	@JsonProperty("user")
	private String user;

	@JsonProperty("password")
	private String password;

	@JsonProperty("url")
	private String url;

	@JsonProperty("host")
	private String host;

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

}
