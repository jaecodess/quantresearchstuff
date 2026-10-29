-- Hive databases for the pipeline (HANDOVER section 9). Safe to rerun on a fresh cluster.
CREATE DATABASE IF NOT EXISTS raw;
CREATE DATABASE IF NOT EXISTS clean;
CREATE DATABASE IF NOT EXISTS features;
CREATE DATABASE IF NOT EXISTS model;
CREATE DATABASE IF NOT EXISTS analytics;
