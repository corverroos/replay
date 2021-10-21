package db

var migrations = []string{`
CREATE TABLE replay_events (
  id BIGINT NOT NULL AUTO_INCREMENT,
  ` + "`key`" + ` VARCHAR(512) NOT NULL,
  namespace VARCHAR(255) NOT NULL, 
  workflow VARCHAR(255) NOT NULL,
  run VARCHAR(255),
  iteration INT NOT NULL,
  type INT NOT NULL,
  timestamp DATETIME(3) NOT NULL,
  message MEDIUMBLOB,

  PRIMARY KEY (id),
  UNIQUE by_type_key (type, ` + "`key`" + `),
  INDEX (type, namespace, workflow, run, iteration)
);
`, `
CREATE TABLE replay_sleeps (
  id BIGINT NOT NULL AUTO_INCREMENT,
  ` + "`key`" + ` VARCHAR(512) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  complete_at DATETIME(3) NOT NULL,
  completed BOOL NOT NULL,

  PRIMARY KEY (id),
  UNIQUE by_key (` + "`key`" + `),
  INDEX (completed, complete_at)
);
`, `
CREATE TABLE replay_signal_awaits (
  id BIGINT NOT NULL AUTO_INCREMENT,
  ` + "`key`" + ` VARCHAR(512) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  timeout_at DATETIME(3) NOT NULL,
  status TINYINT NOT NULL,

  PRIMARY KEY (id),
  UNIQUE by_key (` + "`key`" + `),
  INDEX (status)
);
`, `
CREATE TABLE replay_signals (
  id BIGINT NOT NULL AUTO_INCREMENT,
  namespace VARCHAR(255) NOT NULL,
  hash BINARY(128) NOT NULL,
  workflow VARCHAR(255) NOT NULL,
  run VARCHAR(255) NOT NULL,
  ` + "`signal`" + ` VARCHAR(255) NOT NULL,
  external_id VARCHAR(255) NOT NULL,
  message MEDIUMBLOB,
  created_at DATETIME(3) NOT NULL,
  check_id BIGINT,

  PRIMARY KEY (id),
  UNIQUE uniq (namespace, hash)
);
`,
}
