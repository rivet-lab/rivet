SET NAMES utf8mb4;
CREATE DATABASE IF NOT EXISTS `st_meta`;
CREATE TABLE IF NOT EXISTS `st_meta`.`targets` (db VARCHAR(64) NOT NULL, tbl VARCHAR(64) NOT NULL, captured TINYINT NOT NULL, native TINYINT NOT NULL, family VARCHAR(16) NOT NULL, PRIMARY KEY (db, tbl)) DEFAULT CHARACTER SET utf8mb4;
DROP DATABASE IF EXISTS `ст_база`; -- allow-cyrillic
CREATE DATABASE `ст_база` DEFAULT CHARACTER SET utf8mb4; -- allow-cyrillic
DELETE FROM `st_meta`.`targets` WHERE db = 'ст_база'; -- allow-cyrillic
DROP TABLE IF EXISTS `ст_база`.`orders`; -- allow-cyrillic
CREATE TABLE `ст_база`.`orders` (`id` INT NOT NULL PRIMARY KEY, `amount` INT NOT NULL, `note` VARCHAR(40)) ENGINE=InnoDB; -- allow-cyrillic
INSERT INTO `ст_база`.`orders` SELECT n, n * 10, CONCAT('orders ', n) FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 5) SELECT n FROM s) q; -- allow-cyrillic
INSERT INTO `st_meta`.`targets` VALUES ('ст_база', 'orders', 1, 0, 'cp1251'); -- allow-cyrillic
DROP TABLE IF EXISTS `ст_база`.`audit_log`; -- allow-cyrillic
CREATE TABLE `ст_база`.`audit_log` (`id` INT NOT NULL PRIMARY KEY, `amount` INT NOT NULL, `note` VARCHAR(40)) ENGINE=InnoDB; -- allow-cyrillic
INSERT INTO `ст_база`.`audit_log` SELECT n, n * 10, CONCAT('audit_log ', n) FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 5) SELECT n FROM s) q; -- allow-cyrillic
INSERT INTO `st_meta`.`targets` VALUES ('ст_база', 'audit_log', 0, 0, 'cp1251'); -- allow-cyrillic
DROP TABLE IF EXISTS `ст_база`.`заказы`; -- allow-cyrillic
CREATE TABLE `ст_база`.`заказы` (`id` INT NOT NULL PRIMARY KEY, `сумма` INT NOT NULL, `note` VARCHAR(40)) ENGINE=InnoDB; -- allow-cyrillic
INSERT INTO `ст_база`.`заказы` SELECT n, n * 10, CONCAT('заказы ', n) FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 5) SELECT n FROM s) q; -- allow-cyrillic
INSERT INTO `st_meta`.`targets` VALUES ('ст_база', 'заказы', 1, 1, 'cp1251'); -- allow-cyrillic
DROP TABLE IF EXISTS `ст_база`.`журнал`; -- allow-cyrillic
CREATE TABLE `ст_база`.`журнал` (`id` INT NOT NULL PRIMARY KEY, `сумма` INT NOT NULL, `note` VARCHAR(40)) ENGINE=InnoDB; -- allow-cyrillic
INSERT INTO `ст_база`.`журнал` SELECT n, n * 10, CONCAT('журнал ', n) FROM (WITH RECURSIVE s(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM s WHERE n < 5) SELECT n FROM s) q; -- allow-cyrillic
INSERT INTO `st_meta`.`targets` VALUES ('ст_база', 'журнал', 0, 1, 'cp1251'); -- allow-cyrillic
