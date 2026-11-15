SET SESSION sql_mode='';
SET SESSION time_zone='+00:00';
SET NAMES utf8mb4;
CREATE TABLE tc_edge_mysql_str (id INT PRIMARY KEY,
 c_u CHAR(10) CHARACTER SET utf8mb4, c_l CHAR(10) CHARACTER SET latin1,
 v_u VARCHAR(255) CHARACTER SET utf8mb4, v_l VARCHAR(255) CHARACTER SET latin1,
 v_big VARCHAR(16000) CHARACTER SET utf8mb4,
 tt TINYTEXT CHARACTER SET utf8mb4, tx TEXT CHARACTER SET utf8mb4, mt MEDIUMTEXT CHARACTER SET utf8mb4, lt LONGTEXT CHARACTER SET utf8mb4,
 tx_l TEXT CHARACTER SET latin1);
INSERT INTO tc_edge_mysql_str VALUES
(1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),
(2,'','','','','','','','','',''),
(3,'abcdefghij','abcdefghij',REPEAT('x',255),REPEAT('y',255),REPEAT('z',16000),REPEAT('t',255),REPEAT('u',65535),REPEAT('m',70000),REPEAT('L',70000),REPEAT('l',65535)),
(4,'😀😀😀😀😀😀😀😀😀😀','ÿÿÿÿÿÿÿÿÿÿ',REPEAT('😀',255),REPEAT('é',255),REPEAT('😀',4000),REPEAT('😀',63),REPEAT('😀',16383),'日本語テキスト','Привет мир','éèàüöß'), -- allow-cyrillic
(5,'  a  ','  b  ','  lead','trail  ',' both ','  ','   x   ',' ',' ',' '),
(6,'a\0b','a\0b','n\0ul','n\0ul','n\0ul','n\0ul','n\0ul','n\0ul','n\0ul','n\0ul'),
(7,'\'"\\\n\t','\'"\\\n\t','CRLF\r\nend','tab\there','q''q','"','\\','%','_','a,b'),
(8,'a','a','a','a','a','a','a','a','a','a');
CREATE TABLE tc_edge_mysql_str_lt16m (id INT PRIMARY KEY, mt MEDIUMTEXT CHARACTER SET latin1);
INSERT INTO tc_edge_mysql_str_lt16m VALUES (1,REPEAT('m',1000000));
CREATE TABLE tc_edge_mysql_bin (id INT PRIMARY KEY, b4 BINARY(4), vb VARBINARY(255), vb_big VARBINARY(60000), tb TINYBLOB, bl BLOB, mb MEDIUMBLOB, lb LONGBLOB);
INSERT INTO tc_edge_mysql_bin VALUES
(1,NULL,NULL,NULL,NULL,NULL,NULL,NULL),
(2,'','','','','','',''),
(3,0x00000000,0x00,0x00,0x00,0x00,0x00,0x00),
(4,0xFFFFFFFF,REPEAT(0xFF,255),REPEAT(0xAB,60000),REPEAT(0xFF,255),REPEAT(0xCD,65535),REPEAT(0xEF,100000),REPEAT(0x01,100000)),
(5,0x61,0x6100,0x610062,0x00ff00,0x0aff,0x0d0a,0x2000),
(6,0x00010203,UNHEX(CONCAT(REPEAT('00',1),'01020304050607')),0x00,0x00,0x00,0x00,0x00);
UPDATE tc_edge_mysql_bin SET vb=UNHEX('000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F202122232425262728292A2B2C2D2E2F303132333435363738393A3B3C3D3E3F404142434445464748494A4B4C4D4E4F505152535455565758595A5B5C5D5E5F606162636465666768696A6B6C6D6E6F707172737475767778797A7B7C7D7E7F808182838485868788898A8B8C8D8E8F909192939495969798999A9B9C9D9E9FA0A1A2A3A4A5A6A7A8A9AAABACADAEAFB0B1B2B3B4B5B6B7B8B9BABBBCBDBEBFC0C1C2C3C4C5C6C7C8C9CACBCCCDCECFD0D1D2D3D4D5D6D7D8D9DADBDCDDDEDFE0E1E2E3E4E5E6E7E8E9EAEBECEDEEEFF0F1F2F3F4F5F6F7F8F9FAFBFCFDFE'), bl=UNHEX('FF') WHERE id=6;
CREATE TABLE tc_edge_mysql_enum (id INT PRIMARY KEY, e ENUM('a','b','c'), e_u ENUM('😀','é','with space',''), s SET('a','b','c'), s_u SET('😀','é','x y'), s64 SET('m1','m2','m3','m4','m5','m6','m7','m8','m9','m10','m11','m12','m13','m14','m15','m16','m17','m18','m19','m20','m21','m22','m23','m24','m25','m26','m27','m28','m29','m30','m31','m32','m33','m34','m35','m36','m37','m38','m39','m40','m41','m42','m43','m44','m45','m46','m47','m48','m49','m50','m51','m52','m53','m54','m55','m56','m57','m58','m59','m60','m61','m62','m63','m64'));
INSERT INTO tc_edge_mysql_enum VALUES (1,NULL,NULL,NULL,NULL,NULL),(2,'a','😀','','','m1'),(3,'c','','a,b,c','😀,é,x y','m1,m64'),(4,'b','with space','b','é','m64'),(5,'a','é','a,c','x y',18446744073709551615);
CREATE TABLE tc_edge_mysql_enum_bad (id INT PRIMARY KEY, e ENUM('a','b'));
INSERT INTO tc_edge_mysql_enum_bad VALUES (1,'zzz');
CREATE TABLE tc_edge_mysql_json (id INT PRIMARY KEY, j JSON);
INSERT INTO tc_edge_mysql_json VALUES (1,NULL),(2,'null'),(3,'{}'),(4,'[]'),(5,'{"a":1,"b":[1,2,{"c":null}],"d":"é😀"}'),(6,'18446744073709551615'),(7,'18446744073709551616'),(8,'-9223372036854775808'),(9,'1.7976931348623157e308'),(10,'"x"'),(11,'true'),(12,'0.1'),(13,'{"k":"\\u0000"}'),(14,'9007199254740993'),(15,'1e-400'),(16,'{"é":"😀","":""}');
INSERT INTO tc_edge_mysql_json VALUES (17,CONCAT(REPEAT('[',99),'1',REPEAT(']',99)));
INSERT INTO tc_edge_mysql_json VALUES (18,CONCAT(REPEAT('{"a":',50),'1',REPEAT('}',50)));
