-- Schema generated by truss. DO NOT EDIT.

CREATE TABLE `migrations` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `query_hash` char(64) NOT NULL,
  `schema_hash` char(64) NOT NULL,
  `created_at` datetime(3) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4

CREATE TABLE `replay_events` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `key` varchar(512) NOT NULL,
  `namespace` varchar(255) NOT NULL,
  `workflow` varchar(255) NOT NULL,
  `run` varchar(255) DEFAULT NULL,
  `iteration` int(11) NOT NULL,
  `type` int(11) NOT NULL,
  `timestamp` datetime(3) NOT NULL,
  `message` mediumblob,
  PRIMARY KEY (`id`),
  UNIQUE KEY `by_type_key` (`type`,`key`),
  KEY `type` (`type`,`namespace`,`workflow`,`run`,`iteration`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4

CREATE TABLE `replay_signal_awaits` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `key` varchar(512) NOT NULL,
  `created_at` datetime(3) NOT NULL,
  `timeout_at` datetime(3) NOT NULL,
  `status` tinyint(4) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `by_key` (`key`),
  KEY `status` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4

CREATE TABLE `replay_signals` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `namespace` varchar(255) NOT NULL,
  `hash` binary(128) NOT NULL,
  `workflow` varchar(255) NOT NULL,
  `run` varchar(255) NOT NULL,
  `type` tinyint(4) NOT NULL,
  `external_id` varchar(255) NOT NULL,
  `message` mediumblob,
  `created_at` datetime(3) NOT NULL,
  `check_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq` (`namespace`,`hash`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4

CREATE TABLE `replay_sleeps` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `key` varchar(512) NOT NULL,
  `created_at` datetime(3) NOT NULL,
  `complete_at` datetime(3) NOT NULL,
  `completed` tinyint(1) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `by_key` (`key`),
  KEY `completed` (`completed`,`complete_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4