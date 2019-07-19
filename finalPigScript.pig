
define CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

register /usr/local/pig/contrib/piggybank/java/piggybank.jar;

customerData = load '/user/hduser/jig10122/capstone/graded/CampaignData_full-2.csv' using CSVLoader;


customerDataManipulated = foreach customerData generate (chararray)$0 as clickFlag,(chararray)$1 as openFlag,(chararray)SUBSTRING($272,5,13) as date,(chararray)SUBSTRING($272,0,3) as day,(chararray)(SUBSTRING($272,5,7)=='01'?'January':(SUBSTRING($272,5,7)=='02'?'February':(SUBSTRING($272,5,7)=='03'?'March':(SUBSTRING($272,5,7)=='04'?'April':(SUBSTRING($272,5,7)=='05'?'May':(SUBSTRING($272,5,7)=='06'?'June':(SUBSTRING($272,5,7)=='07'?'July':(SUBSTRING($272,5,7)=='08'?'August':(SUBSTRING($272,5,7)=='09'?'September':(SUBSTRING($272,5,7)=='10'?'October':(SUBSTRING($272,5,7)=='11'?'November':'December'))))))))))) as month,(chararray)CONCAT('20',SUBSTRING($272,11,13)) as year,(chararray)SUBSTRING($272,14,19) as timestamp,(chararray)SUBSTRING($272,20,22) as ampm,(chararray)($60==''?'null':($60 is null?'null':$60)) as gender,(chararray)($101==''?'null':($101 is null?'null':$101)) as income,(chararray)($4==''?'null':($4 is null?'null':$4)) as ethnicity,(chararray)($61==''?'null':($61 is null?'null':$61)) as householdStatus;

customerDataManipulated1 = filter customerDataManipulated by ampm is NOT NULL;
store customerDataManipulated1 into '/user/hduser/jig10122/capstone/graded/pigoutput1' using PigStorage('|');



householdDataManipulated = foreach customerData generate (chararray)($50==''?'null':($50 is null?'null':$50)) as gender0to3,(chararray)($51==''?'null':($51 is null?'null':$51)) as gender10to12,(chararray)($52==''?'null':($52 is null?'null':$52)) as gender13to18,(chararray)($53==''?'null':($53 is null?'null':$53)) as gender4to6,(chararray)($54==''?'null':($54 is null?'null':$54)) as gender7to9,(chararray)($76==''?'null':($76 is null?'null':$76)) as noOfChildren18OrLess,(chararray)($92==''?'null':($92 is null?'null':$92)) as presenceOfChildren,(chararray)($125==''?'null':($125 is null?'null':$125))  as statusmem1,(chararray)($126==''?'null':($126 is null?'null':$126))  as statusmem2,(chararray)($127==''?'null':($127 is null?'null':$127))  as statusmem3,(chararray)($128==''?'null':($128 is null?'null':$128))  as statusmem4,(chararray)($129==''?'null':($129 is null?'null':$129))  as statusmem5,(chararray)($130==''?'null':($130 is null?'null':$130))  as statusmem6,(chararray)($131==''?'null':($131 is null?'null':$131))  as statusmem7,(chararray)($132==''?'null':($132 is null?'null':$132))  as statusmem8 ;

STORE householdDataManipulated into '/user/hduser/jig10122/capstone/graded/pigoutput2' using PigStorage('|');



getStatus = foreach householdDataManipulated generate statusmem1,statusmem2,statusmem3,statusmem4,statusmem5,statusmem6,statusmem7,statusmem8;
store getStatus into '/user/hduser/jig10122/capstone/graded/pigOutStatus/' using PigStorage('|');


getGender = foreach householdDataManipulated generate gender0to3,gender10to12,gender13to18,gender4to6,gender7to9;
store getGender into '/user/hduser/jig10122/capstone/graded/pigOutGender/' using PigStorage('|');


getChildrenDetails = foreach householdDataManipulated generate (int)noOfChildren18OrLess,presenceOfChildren;



getstatus1 = foreach getStatus generate FLATTEN(TOBAG(*)) as hhldtype;
grouped = group getstatus1 by hhldtype;
getTypeCount = foreach grouped generate group as hhldType,COUNT(getstatus1) as total;
store getTypeCount into '/user/hduser/jig10122/capstone/graded/pigOutStatusType' using PigStorage('|');


groupAll = group getTypeCount ALL;
getGrandTotal = foreach groupAll generate SUM(getTypeCount.total) as grandTotal;
getPercentage = foreach getTypeCount generate $0,$1,$1/(float)getGrandTotal.grandTotal * 100 as percent1;
store getPercentage into '/user/hduser/jig10122/capstone/graded/pigOutStatusTypePercent' using PigStorage('|');


getGender = foreach householdDataManipulated generate gender0to3,gender10to12,gender13to18,gender4to6,gender7to9;
getGender1 = foreach getGender generate FLATTEN(TOBAG(*)) as gender;
grouped = group getGender1 by gender;
getGenderCount = foreach grouped generate group as genderType,COUNT(getGender1) as total;
store getGenderCount into '/user/hduser/jig10122/capstone/graded/pigOutGenderType' using PigStorage('|');


getPresenceY = filter getChildrenDetails by presenceOfChildren=='Y';
grouped = group getPresenceY by presenceOfChildren;
getByNoOfChildY = foreach grouped generate group as presenceType,SUM(getPresenceY.noOfChildren18OrLess);
store  getByNoOfChildY into '/user/hduser/jig10122/capstone/graded/pigOutTotalChildren' using PigStorage('|');




