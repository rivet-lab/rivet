USE rivet;
SET NOCOUNT ON;
DROP TABLE IF EXISTS tc_edge_mssql_int, tc_edge_mssql_dec, tc_edge_mssql_flt, tc_edge_mssql_str, tc_edge_mssql_bin, tc_edge_mssql_date, tc_edge_mssql_time, tc_edge_mssql_dt2, tc_edge_mssql_dto, tc_edge_mssql_uid, tc_edge_mssql_xml, tc_edge_mssql_var, tc_edge_mssql_clr, tc_edge_mssql_rv;

CREATE TABLE tc_edge_mssql_int(id int PRIMARY KEY, c_bit bit, c_tiny tinyint, c_small smallint, c_int int, c_big bigint);
INSERT tc_edge_mssql_int VALUES (1,NULL,NULL,NULL,NULL,NULL);
INSERT tc_edge_mssql_int VALUES (2,0,0,-32768,-2147483648,-9223372036854775808);
INSERT tc_edge_mssql_int VALUES (3,1,1,-32767,-2147483647,-9223372036854775807);
INSERT tc_edge_mssql_int VALUES (4,NULL,254,0,0,0);
INSERT tc_edge_mssql_int VALUES (5,NULL,255,32766,2147483646,9223372036854775806);
INSERT tc_edge_mssql_int VALUES (6,NULL,NULL,32767,2147483647,9223372036854775807);

CREATE TABLE tc_edge_mssql_dec(id int PRIMARY KEY, d38_0 decimal(38,0), d38_38 decimal(38,38), d38_10 decimal(38,10), d1_0 decimal(1,0), d18_6 decimal(18,6), n10_2 numeric(10,2), c_money money, c_smallmoney smallmoney);
INSERT tc_edge_mssql_dec VALUES (1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT tc_edge_mssql_dec VALUES (2,0,0,0,0,0,0,0,0);
INSERT tc_edge_mssql_dec VALUES (3,99999999999999999999999999999999999999,0.99999999999999999999999999999999999999,9999999999999999999999999999.9999999999,9,999999999999.999999,99999999.99,922337203685477.5807,214748.3647);
INSERT tc_edge_mssql_dec VALUES (4,-99999999999999999999999999999999999999,-0.99999999999999999999999999999999999999,-9999999999999999999999999999.9999999999,-9,-999999999999.999999,-99999999.99,-922337203685477.5808,-214748.3648);
INSERT tc_edge_mssql_dec VALUES (5,1,0.00000000000000000000000000000000000001,0.0000000001,1,0.000001,0.01,0.0001,0.0001);
INSERT tc_edge_mssql_dec VALUES (6,-1,-0.00000000000000000000000000000000000001,-0.0000000001,-1,-0.000001,-0.01,-0.0001,-0.0001);
INSERT tc_edge_mssql_dec VALUES (7,99999999999999999999999999999999999998,NULL,NULL,NULL,NULL,NULL,NULL,NULL);

CREATE TABLE tc_edge_mssql_flt(id int PRIMARY KEY, c_real real, c_float float);
INSERT tc_edge_mssql_flt VALUES (1,NULL,NULL),(2,0,0);
INSERT tc_edge_mssql_flt VALUES (3,3.40282347E+38,1.7976931348623157E+308);
INSERT tc_edge_mssql_flt VALUES (4,-3.40282347E+38,-1.7976931348623157E+308);
INSERT tc_edge_mssql_flt VALUES (5,1.17549435E-38,2.2250738585072014E-308);
INSERT tc_edge_mssql_flt VALUES (6,1,1),(7,-1,-1),(8,16777217,9007199254740993),(9,0.1,0.1);
INSERT tc_edge_mssql_flt VALUES (10,1.4E-45,NULL);

CREATE TABLE tc_edge_mssql_str(id int PRIMARY KEY, c_char char(10), c_varchar varchar(20), c_varchar_max varchar(max), c_nchar nchar(10), c_nvarchar nvarchar(20), c_nvarchar_max nvarchar(max), c_text text, c_ntext ntext);
INSERT tc_edge_mssql_str VALUES (1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
INSERT tc_edge_mssql_str VALUES (2,'','','',N'',N'',N'','',N'');
INSERT tc_edge_mssql_str VALUES (3,'abc','abc','abc',N'abc',N'abc',N'abc','abc',N'abc');
INSERT tc_edge_mssql_str VALUES (4,' x ',' x ',' x ',N' x ',N' x ',N' x ',' x ',N' x ');
INSERT tc_edge_mssql_str VALUES (5,'abcdefghij','abcdefghijklmnopqrst',REPLICATE(CAST('x' AS varchar(max)),100000),N'abcdefghij',N'abcdefghijklmnopqrst',REPLICATE(CAST(N'y' AS nvarchar(max)),100000),REPLICATE(CAST('t' AS varchar(max)),100000),REPLICATE(CAST(N'n' AS nvarchar(max)),100000));
INSERT tc_edge_mssql_str VALUES (6,'a'+CHAR(0)+'b','a'+CHAR(0)+'b','a'+CHAR(0)+'b',N'a'+NCHAR(0)+N'b',N'a'+NCHAR(0)+N'b',N'a'+NCHAR(0)+N'b','a'+CHAR(0)+'b',N'a'+NCHAR(0)+N'b');
INSERT tc_edge_mssql_str VALUES (7,NULL,NULL,NULL,NCHAR(0xD83D)+NCHAR(0xDE00),NCHAR(0xD83D)+NCHAR(0xDE00)+N'é中',REPLICATE(CAST(NCHAR(0xD83D)+NCHAR(0xDE00) AS nvarchar(max)),50000),NULL,NCHAR(0xD83D)+NCHAR(0xDE00));
INSERT tc_edge_mssql_str VALUES (8,NULL,NULL,NULL,NCHAR(0xD800),NCHAR(0xD800)+N'x',NCHAR(0xDC00),NULL,NCHAR(0xD800));
INSERT tc_edge_mssql_str VALUES (9,CHAR(233),CHAR(233),CHAR(233),NULL,NULL,NULL,CHAR(233),NULL);

DECLARE @b varbinary(max)=0x, @i int=0;
WHILE @i<256 BEGIN SET @b=@b+CAST(CAST(@i AS tinyint) AS binary(1)); SET @i+=1; END
DECLARE @big varbinary(max)=0xAB, @k int=0; WHILE @k<17 BEGIN SET @big=@big+@big; SET @k+=1; END; SET @big=SUBSTRING(@big,1,100000);
CREATE TABLE tc_edge_mssql_bin(id int PRIMARY KEY, c_binary binary(16), c_varbinary varbinary(16), c_varbinary_max varbinary(max), c_image image);
INSERT tc_edge_mssql_bin VALUES (1,NULL,NULL,NULL,NULL);
INSERT tc_edge_mssql_bin VALUES (2,0x00000000000000000000000000000000,0x,0x,0x);
INSERT tc_edge_mssql_bin VALUES (3,0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF,0x00,0x00,0x00);
INSERT tc_edge_mssql_bin VALUES (4,0x01,0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF,@b,@b);
INSERT tc_edge_mssql_bin VALUES (5,NULL,NULL,@big,@big);

CREATE TABLE tc_edge_mssql_date(id int PRIMARY KEY, c_date date, c_datetime datetime, c_smalldatetime smalldatetime);
INSERT tc_edge_mssql_date VALUES (1,NULL,NULL,NULL);
INSERT tc_edge_mssql_date VALUES (2,'0001-01-01','1753-01-01 00:00:00.000','1900-01-01 00:00');
INSERT tc_edge_mssql_date VALUES (3,'9999-12-31','9999-12-31 23:59:59.997','2079-06-06 23:59');
INSERT tc_edge_mssql_date VALUES (4,'1970-01-01','1970-01-01 00:00:00.000','1970-01-01 00:00');
INSERT tc_edge_mssql_date VALUES (5,'1969-12-31','1969-12-31 23:59:59.997','1969-12-31 23:59');
INSERT tc_edge_mssql_date VALUES (6,'1677-09-21','1900-01-01 00:00:00.000','2000-02-29 12:34');
INSERT tc_edge_mssql_date VALUES (7,'2262-04-11','2000-02-29 12:34:56.003',NULL);
INSERT tc_edge_mssql_date VALUES (8,'1900-01-01',NULL,NULL);
INSERT tc_edge_mssql_date VALUES (9,'2299-12-31','2299-12-31 23:59:59.997',NULL);
INSERT tc_edge_mssql_date VALUES (10,'2000-02-29','2262-04-11 23:47:16',NULL);
INSERT tc_edge_mssql_date VALUES (11,'1899-12-31','2000-02-29 12:34:56.007',NULL);
INSERT tc_edge_mssql_date VALUES (12,'2300-01-01',NULL,NULL);

