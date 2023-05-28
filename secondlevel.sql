-- hql中级题目
show databases;

-- 1. 查询销量第二的商品 -> 存在多个，全部返回，不存在，返回null
drop table if exists order_summary;
create table order_summary (
    id varchar(32) comment '商品id',
    category_id varchar(32) comment '商品所属品类id',
    sum int comment '商品销售总额累计',
    test_case int
) comment '订单汇总表'
row format delimited fields terminated by '\t'
null defined as ''
location '/warehouse/sdc/rds/order_summary';

INSERT INTO order_summary VALUES ('1','1',12,1),('2','2',32,1),('3','3',44,1),('4','2',56,1),('1','3',78,2),('1','1',12,3),('2','2',15,3),('3','3',17,3),('4','3',17,3),('5','3',19,3),('1','1',33,4),('2','1',55,4),('3','1',77,4),('4','2',23,4);

select
    *
from order_summary
order by sum desc;

with temp_table as (
    select
        id,
        dense_rank() over (partition by flag order by sum desc) as rank_second
    from (
        select
            *,
            1 as flag
        from order_summary
    ) as table_temp
)
select `if`(exists (select * from temp_table where rank_second = 2), (select distinct id from temp_table where rank_second = 2), null) as id;

-- 2. 连续登录至少三天的用户
drop table if exists user_active;

create table user_active (
    user_id varchar(30) comment '用户id',
    active_date date comment '用户登录日期',
    test_case int
) row format delimited fields terminated by '\t'
null defined as ''
location '/warehouse/sdc/rds/user_active';

INSERT INTO user_active VALUES ('1','2022-04-25',NULL),('1','2022-04-26',NULL),('1','2022-04-27',NULL),('1','2022-04-28',NULL),('1','2022-04-29',NULL),('2','2022-04-12',NULL),('2','2022-04-13',NULL),('3','2022-04-05',NULL),('3','2022-04-06',NULL),('3','2022-04-08',NULL),('4','2022-04-04',NULL),('4','2022-04-05',NULL),('4','2022-04-06',NULL),('4','2022-04-06',NULL),('4','2022-04-06',NULL),('5','2022-04-12',NULL),('5','2022-04-08',NULL),('5','2022-04-26',NULL),('5','2022-04-26',NULL),('5','2022-04-27',NULL),('5','2022-04-28',NULL);

select *
from user_active;

with temp_table as (
    select user_id, active_date
    from user_active
    group by user_id, active_date
)
select
    distinct user_id
from (
    select user_id,
        lag(active_date, 1, null) over (partition by user_id order by active_date asc) as lastay,
        active_date,
        lead(active_date, 1, null) over (partition by user_id order by active_date asc) as nextday
    from temp_table
) as table_temp
where lastay = date_sub(active_date, 1) and date_add(active_date, 1) = nextday
order by user_id asc;

-- 3. 查询各品类共有多少个商品以及销售最多的商品
drop table if exists category;
create table category (
    category_id varchar(32),
    category_name varchar(32)
) comment '品类表'
row format delimited fields terminated by '\t'
null defined as ''
location '/warehouse/sdc/rds/category';

INSERT INTO category VALUES ('1','数码'),('2','日用'),('3','厨房清洁');

select *
from category;

select *
from order_summary;

select *
from order_summary
order by category_id asc, id asc;


with temp_table as (
    select id,
           category_id,
           rank() over (partition by category_id order by product_total_sum desc) as ranking
    from (
        select
            id,
            category_id,
            sum(sum) as product_total_sum
        from order_summary
        group by id, category_id
    ) as table_temp
)
select
    temp_table.category_id,
    product_quantity,
    id
from temp_table
left join (
    select
        category_id,
        count(distinct id) as product_quantity
    from order_summary
    group by category_id
) as table_temp
on table_temp.category_id = temp_table.category_id
where ranking = 1
order by category_id asc, id asc;


select category_id,
       id,
       sum
from order_summary
order by category_id asc, id asc;

select *
from order_summary;

-- 4. 用户的消费明细表中有每个用户每天的消费总额记录，
--    需要汇总形成一张用户累计消费总额，求每个用户在有消费记录的日期之前的所有累计总消费金额以及vip等级
--    vip等级：[0, 1000) 普通会员 -> [1000, 3000) 青铜会员 -> [3000, 5000) 白银会员
--         -> [5000, 8000) 黄金会员 -> [8000, 10000) 白金会员 -> [10000, ∞) 钻石会员

drop table if exists user_consum_details;

create table user_consum_details (
    user_id varchar(32),
    buy_date date,
    sum int
) comment '用户消费明细表'
row format delimited fields terminated by '\t'
null defined as ''
location '/warehouse/sdc/rds/user_consum_details';

INSERT INTO user_consum_details VALUES ('1','2022-04-26',3000),('1','2022-04-27',5000),('1','2022-04-29',1000),('1','2022-04-30',2000),('2','2022-04-27',9000),('2','2022-04-29',6000),('3','2022-04-22',5000);

select *
from user_consum_details;

with temp_table as (
    select
        user_id,
        buy_date,
        sum(sum) over(partition by user_id order by buy_date asc) as sum_total
    from user_consum_details
)
select user_id,
       buy_date,
       sum_total,
       case
           when sum_total >= 10000 then '钻石会员'
           when sum_total >= 8000 then '白金会员'
           when sum_total >= 5000 then '黄金会员'
           when sum_total >= 3000 then '白银会员'
           when sum_total >= 1000 then '青铜会员'
           else '普通会员'
       end as membership_level
from temp_table
order by user_id asc, buy_date asc;


-- 5. 查询首次消费后第二天仍然消费的用户占所有用户的比率，结果保留一位小数，使用百分数表示
select *
from user_consum_details;

with temp_table as (
    select
        user_id,
        buy_date,
        ranking,
        2 as flag_2
    from (
        select *, 1 as flag, rank() over (partition by user_id order by buy_date asc) as ranking
        from user_consum_details
    ) as table_temp
    where ranking <= 2
)
select round(flag_1 * 100, 1) as percent_rate
from (
    select (count(distinct user_id)) / (select count(distinct user_id) from temp_table group by flag_2) as flag_1
    from (
        select
            user_id,
            1 as flag_1
        from temp_table
        where user_id not in (select user_id from temp_table group by user_id having count(*) < 2)
        group by user_id
        having count(distinct date_sub(buy_date, ranking)) = 1
    ) as table_temp
    group by flag_1
) as table_temp;


select 1 * 2 as flag;

-- 6. 订单明细表每行代表一次销售，请计算每个商品第一年的销售数量，销售年份和销售总额

drop table if exists order_detail;

create table order_detail (
    order_id varchar(32) not null comment '订单id',
    id varchar(32) comment '商品id',
    price int comment '订单总额',
    sale_date date comment '商品销售日期',
    num int comment '商品件数'
) comment '订单明细表'
row format delimited fields terminated by '\t'
null defined as ''
location '/warehouse/sdc/rds/order_detail';

INSERT INTO order_detail VALUES ('1','1',80,'2021-12-29',8),('2','1',10,'2021-12-30',1),('3','2',55,'2021-04-30',5),('3','2',55,'2020-04-30',5),('4','3',550,'2021-03-31',10),('5','4',550,'2021-05-04',15),('6','2',30,'2021-08-07',3),('7','2',60,'2020-08-09',6);

select *
from order_detail;

with temp_table as (
    select id, min(year(sale_date)) as first_year
    from order_detail
    group by id
)
select
    order_detail.id,
    first_year,
    sum(num) as num_total,
    sum(price) as price_total
from order_detail
left join temp_table
on order_detail.id = temp_table.id
where year(sale_date) = first_year
group by order_detail.id, first_year
order by order_detail.id asc;

-- 7. 不考虑上架时间小于一个月的商品，假设今天的日期是2022-01-10。请筛选出去年总销量小于10的商品

drop table if exists product_attr;

create table product_attr (
    id varchar(32) comment '商品id',
    name varchar(32) comment '商品名称',
    category_id varchar(32) comment '商品所属品类id',
    from_date date comment '商品上架日期'
) comment '商品属性表'
row format delimited fields terminated by '\t'
null defined as ''
location '/warehouse/sdc/rds/product_attr';

INSERT INTO product_attr VALUES ('1','xiaomi','1','2021-12-23'),('2','apple','1','2020-10-18'),('3','nokia','1','2019-10-29'),('4','vivo','1','2020-02-02');

select *
from product_attr;

select *
from order_detail;

select order_detail.id,
       name
from order_detail
left join product_attr
on order_detail.id = product_attr.id
where datediff('2022-01-10', from_date) > 30 and year(sale_date) = 2021
group by order_detail.id, name
having sum(num) < 10
order by order_detail.id asc;