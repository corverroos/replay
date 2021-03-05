package db

var migrations = []string{`
CREATE TABLE events (
  id BIGINT NOT NULL AUTO_INCREMENT,
  foreign_id VARCHAR(255) NOT NULL,
  workflow VARCHAR(255) NOT NULL DEFAULT "",
  run VARCHAR(255),
  type INT NOT NULL,
  timestamp DATETIME(3) NOT NULL,
  metadata VARCHAR(255),

  PRIMARY KEY (id),
  UNIQUE foreign_id_type (foreign_id, type),
  INDEX (workflow, run, type)
);
`,
}
