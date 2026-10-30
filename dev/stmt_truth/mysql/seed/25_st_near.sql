SET NAMES utf8mb4;
CREATE DATABASE IF NOT EXISTS `st_meta`;
CREATE TABLE IF NOT EXISTS `st_meta`.`targets` (db VARCHAR(64) NOT NULL, tbl VARCHAR(64) NOT NULL, captured TINYINT NOT NULL, native TINYINT NOT NULL, family VARCHAR(16) NOT NULL, PRIMARY KEY (db, tbl)) DEFAULT CHARACTER SET utf8mb4;
DROP DATABASE IF EXISTS `st_near`;
CREATE DATABASE `st_near` DEFAULT CHARACTER SET utf8mb4;
DROP DATABASE IF EXISTS `st_near_to`;
CREATE DATABASE `st_near_to` DEFAULT CHARACTER SET utf8mb4;
DELETE FROM `st_meta`.`targets` WHERE db IN ('st_near', 'st_near_to');
DROP TABLE IF EXISTS `st_near`.`client`;
CREATE TABLE `st_near`.`client` (`id` INT NOT NULL PRIMARY KEY, `amount` INT NOT NULL, `note` VARCHAR(40)) ENGINE=InnoDB;
INSERT INTO `st_near`.`client` SELECT n, n * 10, CONCAT('near ', n) FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 5) SELECT n FROM s) q;
INSERT INTO `st_meta`.`targets` VALUES ('st_near', 'client', 1, 0, 'near');
DROP TABLE IF EXISTS `st_near`.`clientèle`;
CREATE TABLE `st_near`.`clientèle` (`id` INT NOT NULL PRIMARY KEY, `amount` INT NOT NULL, `note` VARCHAR(40)) ENGINE=InnoDB;
INSERT INTO `st_near`.`clientèle` SELECT n, n * 10, CONCAT('near ', n) FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 5) SELECT n FROM s) q;
INSERT INTO `st_meta`.`targets` VALUES ('st_near', 'clientèle', 0, 1, 'near');
DROP TABLE IF EXISTS `st_near`.`clientčle`;
CREATE TABLE `st_near`.`clientčle` (`id` INT NOT NULL PRIMARY KEY, `amount` INT NOT NULL, `note` VARCHAR(40)) ENGINE=InnoDB;
INSERT INTO `st_near`.`clientčle` SELECT n, n * 10, CONCAT('near ', n) FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 5) SELECT n FROM s) q;
INSERT INTO `st_meta`.`targets` VALUES ('st_near', 'clientčle', 0, 1, 'near');
DROP TABLE IF EXISTS `st_near`.`clientы`; -- allow-cyrillic
CREATE TABLE `st_near`.`clientы` (`id` INT NOT NULL PRIMARY KEY, `amount` INT NOT NULL, `note` VARCHAR(40)) ENGINE=InnoDB; -- allow-cyrillic
INSERT INTO `st_near`.`clientы` SELECT n, n * 10, CONCAT('near ', n) FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 5) SELECT n FROM s) q; -- allow-cyrillic
INSERT INTO `st_meta`.`targets` VALUES ('st_near', 'clientы', 0, 1, 'near'); -- allow-cyrillic
DROP TABLE IF EXISTS `st_near_to`.`client`;
CREATE TABLE `st_near_to`.`client` (`id` INT NOT NULL PRIMARY KEY, `amount` INT NOT NULL, `note` VARCHAR(40)) ENGINE=InnoDB;
INSERT INTO `st_near_to`.`client` SELECT n, n * 10, CONCAT('near ', n) FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 5) SELECT n FROM s) q;
INSERT INTO `st_meta`.`targets` VALUES ('st_near_to', 'client', 1, 0, 'near');
DROP TABLE IF EXISTS `st_near_to`.`tmp`;
CREATE TABLE `st_near_to`.`tmp` (`id` INT NOT NULL PRIMARY KEY, `amount` INT NOT NULL, `note` VARCHAR(40)) ENGINE=InnoDB;
INSERT INTO `st_near_to`.`tmp` SELECT n, n * 10, CONCAT('near ', n) FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 5) SELECT n FROM s) q;
INSERT INTO `st_meta`.`targets` VALUES ('st_near_to', 'tmp', 0, 0, 'near');
