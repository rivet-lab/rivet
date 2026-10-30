DROP USER IF EXISTS 'st_acct'@'%';
DROP USER IF EXISTS 'st_tmp'@'%';
DROP USER IF EXISTS 'st_new'@'%';
DROP USER IF EXISTS 'st_tmp2'@'%';
CREATE USER 'st_acct'@'%' IDENTIFIED BY 'st_acct';
CREATE USER 'st_tmp'@'%' IDENTIFIED BY 'st_tmp';
GRANT SELECT (`id`, `amount`) ON `st_ascii`.`orders` TO 'st_acct'@'%';
GRANT SELECT ON `st_ascii`.`audit_log` TO 'st_acct'@'%';
