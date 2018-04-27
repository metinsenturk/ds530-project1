Create table Calendar_Dimension as
select row_number() over(order by dayofmonth asc) as calendarkey, *
from (select distinct tdate as "Full Date",
to_char(tdate,'Day') as dayofweek,
extract (day from tdate) as dayofmonth,
to_char(tdate,'Month') as "month", 'Q' ||
extract (QUARTER from tdate) Qtr,
extract(year from tdate) "year" from salestransaction) cal;

create table product_dimension as
select row_number() over(order by productID asc) as Productkey,
p.productid, p.productname, p.productprice, v.vendorname ProductVendorName, c.categoryname ProductCategoryName
from product p join vendor v on p.vendorid=v.vendorid
join category c on p.categoryid=c.categoryid;

create table store_1 as
select * from store
where storeid in ('S1','S2','S3');

alter table store_1
add column StoreSize int;

update store_1 set storesize='51000'
where storeid = 'S1';
update store_1 set storesize='35000'
where storeid = 'S2';
update store_1 set storesize='55000'
where storeid = 'S3';

create table StoreCheckoutsystem (Storeid char(2) not null,
StoreCSystem varchar(15) not null,
primary key (Storeid));

insert into StoreCheckoutSystem values('S1','Cashiers');
insert into StoreCheckoutSystem values('S2','Self Service');
insert into StoreCheckoutSystem values('S3','Mixed');

create table Storelayout (Storeid char(2) not null,
StoreLayout varchar(15) not null,
primary key (Storeid));

insert into Storelayout values('S1','Modern');
insert into Storelayout values('S2','Traditional');
insert into Storelayout values('S3','Traditional');

--Creating Store Dimension table from Store, StoreCheckoutSystem and StoreLayout tables
create table store_dimension as
select row_number() over(order by s.storeid asc) Storekey, s.storeid, s.storezip, r.regionname, s.storesize, sc.StoreCSystem, sl.StoreLayout
from store_1 as s, region r, Storecheckoutsystem sc, Storelayout sl
where s.storeid=sc.storeid and s.storeid=sl.storeid and s.regionid=r.regionid;

--Creating Customer Dimension table from Customer relational table
create table Customer_Dimension as
select row_number() over(order by customerid asc) Customerkey, * from customer
where customerid in ('1-2-333','2-3-444','3-4-555');

alter table customer_dimension
add CustomerGender varchar;
alter table customer_dimension
add CustomerMaritalStatus varchar;
alter table customer_dimension
add CustomerEducationLevel varchar;
alter table customer_dimension
add CustomerCreditScore int;

update customer_dimension set customergender='Female'
where customerid in ('1-2-333','3-4-555');
update customer_dimension set customergender='Male'
where customerid in ('2-3-444');
update customer_dimension set customermaritalstatus='Single',
CustomerEducationLevel='College',
CustomerCreditScore='700'
where customerid = '1-2-333';
update customer_dimension set customermaritalstatus='Single',
CustomerEducationLevel='High School',
CustomerCreditScore='650'
where customerid = '2-3-444';
update customer_dimension set customermaritalstatus='Married',
CustomerEducationLevel='College',
CustomerCreditScore='623'
where customerid = '3-4-555';
--Adding ttime to salesTransaction table
create table sales_1 as
select * from salestransaction
where storeid in ('S1','S2','S3') and Customerid in ('1-2-333','2-3-444','3-4-555');

alter table sales_1
add column ttime date;

update sales_1
set ttime=current_date
where tid in ('T111','T222','T555');

update sales_1
set ttime=current_date
where tid in ('T333','T444');

--Creating Sales Fact table from sales Transaction and other dimension tables created above
create table sales_fact as
select c.calendarkey, s.storekey, p.productkey, cu.customerkey, st.tid, st.ttime, (p.productprice * sum(sv.noofitems)) DollarsSold,sum(sv.noofitems) UnitsSold
from sales_1 st, soldvia sv, calendar_dimension c, store_dimension s, product_dimension p, customer_dimension cu
where st.customerid=cu.customerid and st.storeid=s.storeid and st.tdate=c."Full Date" and sv.productid = p.productid and st.tid=sv.tid
group by c.calendarkey, s.storekey, p.productkey, cu.customerkey, st.tid, st.ttime, p.productprice
order by c.calendarkey, s.storekey, p.productkey, cu.customerkey, st.tid, st.ttime;

create table aggregated_fact as
select calendarkey, storekey, productkey, sum(dollarssold) TotalDollarsSold, sum(unitssold) TotalUnitsSold
from sales_fact
group by calendarkey, storekey, productkey
order by calendarkey, storekey, productkey;
