
create database jig10122;

set hive.cli.print.current.db=true;

use jig10122;

create external table customerCTOR
(clickFlag string,
openFlag string,
date string,
day string,
month string,
year int,
timestamp string,
ampm string,
gender string,
income string,
ethnicity string,
householdStatus string)
row format delimited 
fields terminated by '|'
lines terminated by '\n'
stored as textfile
location '/user/hduser/jig10122/capstone/graded/pigoutput1';



create external table configdetail
(genderCode map<string,string>,
ethnicityCode map<string,string>,
hhldstatCode map<string,string>,
incomeGrpCode map<string,string>,
noOfChildrenCode map<string,string>
)
row format delimited
fields terminated by '@'
collection items terminated by '#'
map keys terminated by '*'
lines terminated by '\n'                                        
stored as textfile                                              
location '/user/hduser/jig10122/capstone/graded/configfile';





create table cust_ClickToOpenRatio as                                                  
select                                                                                 
ROUND(SUM(CASE WHEN clickFlag=='Y' THEN 1 ELSE 0 END)/COUNT(*) * 100,2) as overallCTOR 
from customerCTOR                                                                      
where openFlag='Y' ;




create table cust_ClickToOpenRatio_Gender as                                                  
select  gender,                                                                               
ROUND(SUM(CASE WHEN clickFlag=='Y' THEN 1 ELSE 0 END)/COUNT(*) * 100,2) as overallCTOR 
from customerCTOR                                                                      
where openFlag='Y' 
group by gender ;


create table final_custCTORGender as 
select c.*,a.overallctor from (select b.gender,b.desc from configdetail c lateral view explode(c.genderCode)b as gender,desc)c
full outer join cust_clicktoopenratio_gender a
on a.gender=c.gender;


create table cust_ClickToOpenRatio_Time as                                                  
select  timestamp,                                                                               
ROUND(SUM(CASE WHEN clickFlag=='Y' THEN 1 ELSE 0 END)/COUNT(*) * 100,2) as overallCTOR 
from customerCTOR                                                                      
where openFlag='Y' 
group by timestamp ;



create table cust_ClickToOpenRatio_DayWeek as                                                  
select  day,                                                                               
ROUND(SUM(CASE WHEN clickFlag=='Y' THEN 1 ELSE 0 END)/COUNT(*) * 100,2) as overallCTOR 
from customerCTOR                                                                      
where openFlag='Y' 
group by day ;


create table cust_ClickToOpenRatio_Month as                                                  
select  month,                                                                               
ROUND(SUM(CASE WHEN clickFlag=='Y' THEN 1 ELSE 0 END)/COUNT(*) * 100,2) as overallCTOR 
from customerCTOR                                                                      
where openFlag='Y' 
group by month ;



create table cust_ClickToOpenRatio_Income as                                                  
select  income,                                                                               
ROUND(SUM(CASE WHEN clickFlag=='Y' THEN 1 ELSE 0 END)/COUNT(*) * 100,2) as overallCTOR 
from customerCTOR                                                                      
where openFlag='Y' 
group by income ;

create table final_custCTORIncomeGrp as 
select c.*,a.overallctor from (select b.incomeGrp,b.desc from configdetail a lateral view explode(a.incomeGrpCode)b as incomeGrp,desc)c
full outer join cust_clicktoopenratio_income a
on a.income=c.incomeGrp;

create table cust_ClickToOpenRatio_Ethnicity as                                                  
select  ethnicity,                                                                               
ROUND(SUM(CASE WHEN clickFlag=='Y' THEN 1 ELSE 0 END)/COUNT(*) * 100,2) as overallCTOR 
from customerCTOR                                                                      
where openFlag='Y' 
group by ethnicity ;

create table final_custCTOREthnicity as 
select c.*,a.overallctor from (select b.ethnicity,b.desc from configdetail a lateral view explode(a.ethnicityCode)b as ethnicity,desc)c
full outer join cust_clicktoopenratio_ethnicity a
on a.ethnicity=c.ethnicity;


create table cust_ClickToOpenRatio_HHStatus as                                                  
select  householdstatus,                                                                               
ROUND(SUM(CASE WHEN clickFlag=='Y' THEN 1 ELSE 0 END)/COUNT(*) * 100,2) as overallCTOR 
from customerCTOR                                                                      
where openFlag='Y' 
group by householdstatus;


create table final_custCTORHHLDStatus as 
select c.*,a.overallctor from (select b.hhldstat,b.desc from configdetail a lateral view explode(a.hhldstatCode)b as hhldstat,desc)c
full outer join cust_clicktoopenratio_hhstatus a
on a.householdstatus=c.hhldstat;






create external table customerHousehold
(gender0to3 string,
gender10to12 string,
gender13to18 string,
gender4to6 string,
gender7to9 string,
noOfChildren18OrLess string,
presenceOfChildren string,
statusmem1 string,
statusmem2 string,
statusmem3 string,
statusmem4 string,
statusmem5 string,
statusmem6 string,
statusmem7 string,
statusmem8 string)
row format delimited 
fields terminated by '|'
lines terminated by '\n'
stored as textfile
location '/user/hduser/jig10122/capstone/graded/pigoutput2';






create view hhm_binary_1 as
select  
(CASE WHEN (statusmem1=='null' OR statusmem1=='U') then 0   
ELSE 1 END) as hhm1,
(CASE WHEN (statusmem2=='null' OR statusmem2=='U') then 0   
ELSE 1 END) as hhm2,
(CASE WHEN (statusmem3=='null' OR statusmem3=='U') then 0   
ELSE 1 END) as hhm3,
(CASE WHEN (statusmem4=='null' OR statusmem4=='U') then 0   
ELSE 1 END) as hhm4,
(CASE WHEN (statusmem5=='null' OR statusmem5=='U') then 0   
ELSE 1 END) as hhm5,
(CASE WHEN (statusmem6=='null' OR statusmem6=='U') then 0   
ELSE 1 END) as hhm6,
(CASE WHEN (statusmem7=='null' OR statusmem7=='U') then 0   
ELSE 1 END) as hhm7,
(CASE WHEN (statusmem8=='null' OR statusmem8=='U') then 0   
ELSE 1 END) as hhm8
from customerHousehold;



create table leadCount_HHLDInfoAvailable as select SUM(hhm1 + hhm2 + hhm3 + hhm4 + hhm5 + hhm6 + hhm7 + hhm8) as total from hhm_binary_1;





create view hhm_binary_2 as
select  
(CASE WHEN (statusmem1=='null') then 0   
ELSE 1 END) as hhm1,
(CASE WHEN (statusmem2=='null') then 0   
ELSE 1 END) as hhm2,
(CASE WHEN (statusmem3=='null') then 0   
ELSE 1 END) as hhm3,
(CASE WHEN (statusmem4=='null') then 0   
ELSE 1 END) as hhm4,
(CASE WHEN (statusmem5=='null') then 0   
ELSE 1 END) as hhm5,
(CASE WHEN (statusmem6=='null' ) then 0   
ELSE 1 END) as hhm6,
(CASE WHEN (statusmem7=='null' ) then 0   
ELSE 1 END) as hhm7,
(CASE WHEN (statusmem8=='null' ) then 0   
ELSE 1 END) as hhm8
from customerHousehold;


create table leadTotal_HHLDInfoAvailable as select SUM(hhm1 + hhm2 + hhm3 + hhm4 + hhm5 + hhm6 + hhm7 + hhm8) as total from hhm_binary_2;





create external table custHHLDByType
 (hhldCode string,totalNum int)
 comment 'Description of household types and their numbers'
 row format delimited
 fields terminated by '\t'
 lines terminated by '\n'
 stored as textfile
 location '/user/hduser/jig10122/capstone/graded/mapreduceStatusType';


create table final_custHHLDByType as 
select a.hhldCode,a.totalNum,c.desc from (select b.hhldstat,b.desc from configdetail a lateral view explode(a.hhldstatCode)b as hhldstat,desc)c
join custHHLDByType a
on a.hhldCode=c.hhldstat;


create external table custHHLDByTypePercent
 (hhldCode string,
totalNum int,
percentage float)
 comment 'Description of household types and their numbers'
 row format delimited
 fields terminated by '|'
 lines terminated by '\n'
 stored as textfile
 location '/user/hduser/jig10122/capstone/graded/pigOutStatusTypePercent' ;


create table final_custHHLDByTypePercent as 
select a.hhldCode,a.totalNum,a.percentage,c.desc from (select b.hhldstat,b.desc from configdetail a lateral view explode(a.hhldstatCode)b as hhldstat,desc)c
full outer join custHHLDByTypePercent a
on a.hhldCode=c.hhldstat;




create table custHHLDPresenceY as select count(*) as NoOfHHLSWithChildren from customerhousehold where presenceofchildren = 'Y';



create table hivetotalChildren_PresenceY as select SUM(CAST(noofchildren18orless AS TINYINT)) from customerhousehold where presenceofchildren='Y';



create external table custHHLDByGender
(genderCode string,
totalNum int)
 comment 'Description of gender types and their numbers'
 row format delimited
 fields terminated by '|'
 lines terminated by '\n'
 stored as textfile
 location '/user/hduser/jig10122/capstone/graded/pigOutGenderType';


create table final_custHHLDByGenderMF as 
select a.genderCode,a.totalNum,c.desc  from (select b.gender,b.desc from configdetail c lateral view explode(c.genderCode)b as gender,desc)c
full outer join custHHLDByGender a
on a.genderCode=c.gender
where a.genderCode='M' or a.genderCode='F';

