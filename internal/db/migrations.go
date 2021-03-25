package db

var migrations = []string{`
CREATE TABLE replay_events (
  id BIGINT NOT NULL AUTO_INCREMENT,
  ` + "`key`" + ` VARCHAR(255) NOT NULL,
  workflow VARCHAR(255) NOT NULL DEFAULT "",
  run VARCHAR(255),
  type INT NOT NULL,
  timestamp DATETIME(3) NOT NULL,
  metadata MEDIUMBLOB,

  PRIMARY KEY (id),
  UNIQUE by_type_key (type, ` + "`key`" + `),
  INDEX (type, workflow, run)
);
`, `
CREATE TABLE replay_sleeps (
  id BIGINT NOT NULL AUTO_INCREMENT,
  ` + "`key`" + ` VARCHAR(255) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  complete_at DATETIME(3) NOT NULL,
  completed BOOL NOT NULL,

  PRIMARY KEY (id),
  UNIQUE by_key (` + "`key`" + `),
  INDEX (completed, complete_at)
);
`, `
CREATE TABLE replay_signal_checks (
  id BIGINT NOT NULL AUTO_INCREMENT,
  ` + "`key`" + ` VARCHAR(255) NOT NULL,
  created_at DATETIME(3) NOT NULL,
  fail_at DATETIME(3) NOT NULL,
  status TINYINT NOT NULL,

  PRIMARY KEY (id),
  UNIQUE by_key (` + "`key`" + `),
  INDEX (status)
);
`, `
CREATE TABLE replay_signals (
  id BIGINT NOT NULL AUTO_INCREMENT,
  workflow VARCHAR(255) NOT NULL,
  run VARCHAR(255) NOT NULL,
  type TINYINT NOT NULL,
  external_id VARCHAR(255) NOT NULL,
  message MEDIUMBLOB,
  created_at DATETIME(3) NOT NULL,
  check_id BIGINT,

  PRIMARY KEY (id),
  UNIQUE uniq (workflow, run, type, external_id)
);
`,
}
