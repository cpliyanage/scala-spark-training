clickDeatails = load 'clickstream' as (customer_atg_id:int, date_visit:chararray, event:chararray);
grouped = group clickDeatails by customer_atg_id;
countData = foreach grouped {
	generate group, COUNT(clickDeatails.customer_atg_id) as cunt: long;};


cusDetails = load 'users' as (customer_atg_id:int, customer_name:chararray);

joinData = join countData by group, cusDetails by customer_atg_id;

orderData = ORDER joinData BY cunt DESC;

filterData = foreach orderData generate customer_name,group;


store filterData into '/tmp/Akash/Traning/Answer' using PigStorage('\t');