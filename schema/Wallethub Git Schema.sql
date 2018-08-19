--- This is some of the work provided for the WalletHub Test ----

--- WalletHub Git Schema -----


--- First Table -----

CREATE TABLE IF NOT EXISTS `LogRecords` (
logtime timestamp(3),
ip varbinary(16),
request varchar(30),
statusvalue smallint,
useragent varchar(500),
primary key(ip,logtime)
);


--- Second Table -----

CREATE TABLE IF NOT EXISTS `IPLog`(
ip varbinary(16),
reason varchar(35),primary key(ip)
);


--- Additional Table -----

CREATE TABLE IF NOT EXISTS `wallethub` (
  `startdate` TIMESTAMP NOT NULL,
  `IP` VARCHAR(20) NOT NULL,
  `request` VARCHAR(20) NOT NULL,
  `threshold` int(50),
  `useragent` VARCHAR(255) NOT NULL
);

--- Examples of the Load File -----

String load = 
"LOAD DATA LOCAL INFILE 'X:/access.log/' REPLACE INTO TABLE `logrecords" + "`\n"
+ "FIELDS TERMINATED BY \'|\'\n" + "ENCLOSED BY \'\"\'\n" + "ESCAPED BY \'\\\\\'\n"
+ "LINES TERMINATED BY \'\\r\\n\'(\n" + "`startDate` ,\n" + "`IP` ,\n" + "`request` ,\n"
+ "`threshold` ,\n" + "`useragent`\n" + ")";


--- One -----

LOAD DATA LOCAL INFILE 'X:\\access.log' REPLACE INTO TABLE `LogRecords`
FIELDS TERMINATED BY '|'
ENCLOSED BY '"'
ESCAPED BY '\\'
LINES TERMINATED BY '\r\n'(
`startDate`,
`IP`,
`request`,
`threshold`,
`useragent`
);

--- Two -----

LOAD DATA LOCAL INFILE 'X:\\access.log' REPLACE INTO TABLE `LogRecords`
FIELDS TERMINATED BY '|'
ENCLOSED BY '"'
ESCAPED BY '\\'
LINES TERMINATED BY '\r\n'(
`logtime`,
`IP`,
`request`,
`statusvalue`,
`useragent`
);

--- Query 1 -----

SELECT inet_ntoa( ip ) AS IPAddress
FROM logrecords
WHERE logtime
BETWEEN '2017-01-01.13:00:00'
AND '2017-01-01.14:00:00'
GROUP BY ip
HAVING count( * ) >=100
LIMIT 0 , 30

-- Results from Query 1 -----

IPAddress
192.168.77.101
192.168.228.188

--- Query 2 ----

SELECT inet_ntoa( ip ) AS IPAddress, logtime, request, statusvalue, useragent
FROM logrecords
WHERE inet_ntoa( ip ) = '192.168.11.231'
LIMIT 0 , 30

-- There are 209 records -----

-- Results from Query 2 for just 3 records -----

IPAddress 	logtime 	request 	statusvalue 	useragent
192.168.11.231 	2017-01-01 15:00:15 	"GET / HTTP/1.1" 	200 	"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/5...
192.168.11.231 	2017-01-01 15:00:33 	"GET / HTTP/1.1" 	200 	"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/5...
192.168.11.231 	2017-01-01 15:00:39 	"GET / HTTP/1.1" 	200 	"Mozilla/5.0 (Windows NT 6.1; WOW64) 
