package db

var migrations = []string{`
CREATE TABLE events (
  id BIGINT NOT NULL AUTO_INCREMENT,
  workflow VARCHAR(255) NOT NULL,
  type INT NOT NULL,
  timestamp DATETIME(3) NOT NULL,
  metadata VARCHAR(255),

  PRIMARY KEY (id)
);
`,
}
