SET NAMES utf8mb4;
CREATE DATABASE IF NOT EXISTS `st_meta`;
CREATE TABLE IF NOT EXISTS `st_meta`.`targets` (db VARCHAR(64) NOT NULL, tbl VARCHAR(64) NOT NULL, captured TINYINT NOT NULL, native TINYINT NOT NULL, family VARCHAR(16) NOT NULL, PRIMARY KEY (db, tbl)) DEFAULT CHARACTER SET utf8mb4;
DROP DATABASE IF EXISTS `st_cyril`;
CREATE DATABASE `st_cyril` DEFAULT CHARACTER SET utf8mb4;
DELETE FROM `st_meta`.`targets` WHERE db = 'st_cyril';
DROP TABLE IF EXISTS `st_cyril`.`orders`;
CREATE TABLE `st_cyril`.`orders` (`id` INT NOT NULL PRIMARY KEY, `amount` INT NOT NULL, `note` VARCHAR(40)) ENGINE=InnoDB;
INSERT INTO `st_cyril`.`orders` SELECT n, n * 10, CONCAT('orders ', n) FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 5) SELECT n FROM s) q;
INSERT INTO `st_meta`.`targets` VALUES ('st_cyril', 'orders', 1, 0, 'cp1251');
DROP TABLE IF EXISTS `st_cyril`.`audit_log`;
CREATE TABLE `st_cyril`.`audit_log` (`id` INT NOT NULL PRIMARY KEY, `amount` INT NOT NULL, `note` VARCHAR(40)) ENGINE=InnoDB;
INSERT INTO `st_cyril`.`audit_log` SELECT n, n * 10, CONCAT('audit_log ', n) FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 5) SELECT n FROM s) q;
INSERT INTO `st_meta`.`targets` VALUES ('st_cyril', 'audit_log', 0, 0, 'cp1251');
DROP TABLE IF EXISTS `st_cyril`.`заказы`; -- allow-cyrillic
CREATE TABLE `st_cyril`.`заказы` (`id` INT NOT NULL PRIMARY KEY, `сумма` INT NOT NULL, `note` VARCHAR(40)) ENGINE=InnoDB; -- allow-cyrillic
INSERT INTO `st_cyril`.`заказы` SELECT n, n * 10, CONCAT('заказы ', n) FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 5) SELECT n FROM s) q; -- allow-cyrillic
INSERT INTO `st_meta`.`targets` VALUES ('st_cyril', 'заказы', 1, 1, 'cp1251'); -- allow-cyrillic
DROP TABLE IF EXISTS `st_cyril`.`журнал`; -- allow-cyrillic
CREATE TABLE `st_cyril`.`журнал` (`id` INT NOT NULL PRIMARY KEY, `сумма` INT NOT NULL, `note` VARCHAR(40)) ENGINE=InnoDB; -- allow-cyrillic
INSERT INTO `st_cyril`.`журнал` SELECT n, n * 10, CONCAT('журнал ', n) FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 5) SELECT n FROM s) q; -- allow-cyrillic
INSERT INTO `st_meta`.`targets` VALUES ('st_cyril', 'журнал', 0, 1, 'cp1251'); -- allow-cyrillic
