package db

var migrations = []string{`
CREATE TABLE events (
  id BIGINT NOT NULL AUTO_INCREMENT,
  `+"`key`"+` VARCHAR(255) NOT NULL,
  workflow VARCHAR(255) NOT NULL DEFAULT "",
  run VARCHAR(255),
  type INT NOT NULL,
  timestamp DATETIME(3) NOT NULL,
  metadata MEDIUMBLOB,

  PRIMARY KEY (id),
  UNIQUE by_type_key (type, `+"`key`"+`),
  INDEX (type, workflow, run)
);
`,`
CREATE TABLE sleeps (
  id BIGINT NOT NULL AUTO_INCREMENT,
  `+"`key`"+` VARCHAR(255) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  complete_at DATETIME(3) NOT NULL,
  completed BOOL NOT NULL,

  PRIMARY KEY (id),
  UNIQUE by_key (`+"`key`"+`),
  INDEX (completed, complete_at)
);
`,
}
