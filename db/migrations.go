package db

var migrations = []string{`
CREATE TABLE events (
  id BIGINT NOT NULL AUTO_INCREMENT,
  foreign_id VARCHAR(255) NOT NULL,
  workflow VARCHAR(255) NOT NULL DEFAULT "",
  run VARCHAR(255),
  type INT NOT NULL,
  timestamp DATETIME(3) NOT NULL,
  metadata MEDIUMBLOB,

  PRIMARY KEY (id),
  UNIQUE foreign_id_type (foreign_id, type),
  INDEX (workflow, run, type)
);
`,`
CREATE TABLE sleeps (
  id BIGINT NOT NULL AUTO_INCREMENT,
  foreign_id VARCHAR(255) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  complete_at DATETIME(3) NOT NULL,
  completed BOOL NOT NULL,

  PRIMARY KEY (id),
  UNIQUE foreign_id (foreign_id),
  INDEX (completed, complete_at)
);
`,
}
