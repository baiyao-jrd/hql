-- hql中级题目
show databases;

-- 1. 查询销量第二的商品 -> 存在多个，全部返回，不存在，返回null
drop table if exists order_summary;
create table order_summary
(
    id          varchar(32) comment '商品id',
    category_id varchar(32) comment '商品所属品类id',
    sum         int comment '商品销售总额累计',
    test_case   int
) comment '订单汇总表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/order_summary';

INSERT INTO order_summary
VALUES ('1', '1', 12, 1),
       ('2', '2', 32, 1),
       ('3', '3', 44, 1),
       ('4', '2', 56, 1),
       ('1', '3', 78, 2),
       ('1', '1', 12, 3),
       ('2', '2', 15, 3),
       ('3', '3', 17, 3),
       ('4', '3', 17, 3),
       ('5', '3', 19, 3),
       ('1', '1', 33, 4),
       ('2', '1', 55, 4),
       ('3', '1', 77, 4),
       ('4', '2', 23, 4);

select *
from order_summary
order by sum desc;

with temp_table as (
    select id,
           dense_rank() over (partition by flag order by sum desc) as rank_second
    from (
             select *,
                    1 as flag
             from order_summary
         ) as table_temp
)
select `if`(exists (select * from temp_table where rank_second = 2),
            (select distinct id from temp_table where rank_second = 2), null) as id;

-- 2. 连续登录至少三天的用户
drop table if exists user_active;

create table user_active
(
    user_id     varchar(30) comment '用户id',
    active_date date comment '用户登录日期',
    test_case   int
) row format delimited fields terminated by '\t'
    null defined as ''
    location '/warehouse/sdc/rds/user_active';

INSERT INTO user_active
VALUES ('1', '2022-04-25', NULL),
       ('1', '2022-04-26', NULL),
       ('1', '2022-04-27', NULL),
       ('1', '2022-04-28', NULL),
       ('1', '2022-04-29', NULL),
       ('2', '2022-04-12', NULL),
       ('2', '2022-04-13', NULL),
       ('3', '2022-04-05', NULL),
       ('3', '2022-04-06', NULL),
       ('3', '2022-04-08', NULL),
       ('4', '2022-04-04', NULL),
       ('4', '2022-04-05', NULL),
       ('4', '2022-04-06', NULL),
       ('4', '2022-04-06', NULL),
       ('4', '2022-04-06', NULL),
       ('5', '2022-04-12', NULL),
       ('5', '2022-04-08', NULL),
       ('5', '2022-04-26', NULL),
       ('5', '2022-04-26', NULL),
       ('5', '2022-04-27', NULL),
       ('5', '2022-04-28', NULL);

select *
from user_active;

with temp_table as (
    select user_id, active_date
    from user_active
    group by user_id, active_date
)
select distinct user_id
from (
         select user_id,
                lag(active_date, 1, null) over (partition by user_id order by active_date asc)  as lastay,
                active_date,
                lead(active_date, 1, null) over (partition by user_id order by active_date asc) as nextday
         from temp_table
     ) as table_temp
where lastay = date_sub(active_date, 1)
  and date_add(active_date, 1) = nextday
order by user_id asc;

-- 3. 查询各品类共有多少个商品以及销售最多的商品
drop table if exists category;
create table category
(
    category_id   varchar(32),
    category_name varchar(32)
) comment '品类表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/category';

INSERT INTO category
VALUES ('1', '数码'),
       ('2', '日用'),
       ('3', '厨房清洁');

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
             select id,
                    category_id,
                    sum(sum) as product_total_sum
             from order_summary
             group by id, category_id
         ) as table_temp
)
select temp_table.category_id,
       product_quantity,
       id
from temp_table
         left join (
    select category_id,
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

create table user_consum_details
(
    user_id  varchar(32),
    buy_date date,
    sum      int
) comment '用户消费明细表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/user_consum_details';

INSERT INTO user_consum_details
VALUES ('1', '2022-04-26', 3000),
       ('1', '2022-04-27', 5000),
       ('1', '2022-04-29', 1000),
       ('1', '2022-04-30', 2000),
       ('2', '2022-04-27', 9000),
       ('2', '2022-04-29', 6000),
       ('3', '2022-04-22', 5000);

select *
from user_consum_details;

with temp_table as (
    select user_id,
           buy_date,
           sum(sum) over (partition by user_id order by buy_date asc) as sum_total
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
    select user_id,
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
                  select user_id,
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

create table order_detail
(
    order_id  varchar(32) not null comment '订单id',
    id        varchar(32) comment '商品id',
    price     int comment '订单总额',
    sale_date date comment '商品销售日期',
    num       int comment '商品件数'
) comment '订单明细表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/order_detail';

INSERT INTO order_detail
VALUES ('1', '1', 80, '2021-12-29', 8),
       ('2', '1', 10, '2021-12-30', 1),
       ('3', '2', 55, '2021-04-30', 5),
       ('3', '2', 55, '2020-04-30', 5),
       ('4', '3', 550, '2021-03-31', 10),
       ('5', '4', 550, '2021-05-04', 15),
       ('6', '2', 30, '2021-08-07', 3),
       ('7', '2', 60, '2020-08-09', 6);

select *
from order_detail;

with temp_table as (
    select id, min(year(sale_date)) as first_year
    from order_detail
    group by id
)
select order_detail.id,
       first_year,
       sum(num)   as num_total,
       sum(price) as price_total
from order_detail
         left join temp_table
                   on order_detail.id = temp_table.id
where year(sale_date) = first_year
group by order_detail.id, first_year
order by order_detail.id asc;

-- 7. 不考虑上架时间小于一个月的商品，假设今天的日期是2022-01-10。请筛选出去年总销量小于10的商品

drop table if exists product_attr;

create table product_attr
(
    id          varchar(32) comment '商品id',
    name        varchar(32) comment '商品名称',
    category_id varchar(32) comment '商品所属品类id',
    from_date   date comment '商品上架日期'
) comment '商品属性表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/product_attr';

INSERT INTO product_attr
VALUES ('1', 'xiaomi', '1', '2021-12-23'),
       ('2', 'apple', '1', '2020-10-18'),
       ('3', 'nokia', '1', '2019-10-29'),
       ('4', 'vivo', '1', '2020-02-02');

select *
from product_attr;

select *
from order_detail;

select order_detail.id,
       name
from order_detail
         left join product_attr
                   on order_detail.id = product_attr.id
where datediff('2022-01-10', from_date) > 30
  and year(sale_date) = 2021
group by order_detail.id, name
having sum(num) < 10
order by order_detail.id asc;

-- 8. 查询从今天之前的90天内，每个日期当天登陆的新用户（新用户定义为在本次登陆之前从未有过登录记录），假设今天是2022-01-10

drop table if exists user_login_detail;

create table user_login_detail
(
    user_id     varchar(32) comment '用户id',
    active_date date comment '登录时间'
) comment '用户登录明细表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/user_login_detail';

INSERT INTO user_login_detail
VALUES ('1', '2020-01-03'),
       ('2', '2021-10-23'),
       ('3', '2021-12-23'),
       ('3', '2021-11-09'),
       ('2', '2021-11-09'),
       ('1', '2021-11-09'),
       ('4', '2021-09-09'),
       ('5', '2021-01-09'),
       ('6', '2021-12-23');

select *
from user_login_detail
order by user_id asc, active_date asc;

with temp_table as (
    select user_id,
           min(active_date) as first_login_date
    from user_login_detail
    group by user_id
)
select user_login_detail.user_id,
       active_date
from user_login_detail
         left join temp_table
                   on user_login_detail.user_id = temp_table.user_id
where active_date >= date_sub('2022-01-10', 90)
  and first_login_date = active_date
order by user_login_detail.user_id asc;

set hive.execution.engine = spark;

-- 9. 订单明细表 -> 求每件商品销售件数最多的日期，如果有销售数量并列的情况，取最小日期，结果按照商品id增序排序

drop table if exists order_detail2;

create table order_detail2
(
    order_id  varchar(32) not null comment '订单id',
    id        varchar(32) comment '商品id',
    price     int comment '订单总额',
    num       int comment '商品件数',
    sale_date date comment '商品销售日期'
) comment '订单明细表2'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/order_detail2';

INSERT INTO order_detail2
VALUES ('1', '1', 80, 8, '2021-12-29'),
       ('2', '1', 10, 1, '2021-12-30'),
       ('3', '2', 55, 5, '2021-04-30'),
       ('3', '2', 55, 5, '2021-04-30'),
       ('4', '3', 550, 10, '2021-03-31'),
       ('5', '4', 550, 15, '2021-05-04'),
       ('6', '2', 30, 3, '2021-08-07'),
       ('7', '2', 60, 6, '2020-08-09'),
       ('8', '4', 550, 15, '2021-05-05');

select *
from order_detail2;

with temp_table as (
    select id,
           sale_date,
           sum(num) as total_num
    from order_detail2
    group by id, sale_date
)
select id,
       sale_date,
       total_num
from (
         select id,
                sale_date,
                total_num,
                rank() over (partition by id order by total_num desc, sale_date asc) as ranking
         from temp_table
     ) as table_temp
where ranking = 1
order by id asc;

-- 10. 查询销售件数高于品类平均数的商品，按照商品id排序

drop table if exists order_detail3;

create table order_detail3
(
    order_id  varchar(32) comment '订单id',
    id        varchar(32) comment '商品id',
    num       int comment '商品件数',
    user_id   varchar(32) comment '用户id',
    sale_date date comment '商品销售日期'
) comment '订单明细表3'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/order_detail3';

drop table if exists product_attr2;

create table product_attr2
(
    id          varchar(32) comment '商品id',
    name        varchar(32) comment '商品名称',
    category_id varchar(32) comment '商品所属品类id'
) comment '商品属性表2'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/product_attr2';

INSERT INTO order_detail3
VALUES ('1', '1', 10, '1', '2021-04-06'),
       ('2', '1', 10, '2', '2021-04-06'),
       ('3', '2', 5, '3', '2021-04-06'),
       ('4', '3', 17, '4', '2021-04-06'),
       ('5', '4', 10, '5', '2021-04-06'),
       ('6', '5', 10, '6', '2021-04-06'),
       ('7', '6', 5, '1', '2021-04-06'),
       ('8', '7', 15, '2', '2021-04-06');

INSERT INTO product_attr2
VALUES ('1', 'xiaomi', '1\r'),
       ('2', 'apple', '1\r'),
       ('3', 'vivo', '1\r'),
       ('4', 'jianbing', '2\r'),
       ('5', 'jiaozi', '2\r'),
       ('6', 'bingxiang', '3\r'),
       ('7', 'xiyiji', '3\r');

select *
from order_detail3;

select *
from product_attr2;

with temp_table as (
    select category_id,
           order_detail3.id,
           name,
           sum(num) as total_num
    from order_detail3
             left join product_attr2
                       on order_detail3.id = product_attr2.id
    group by order_detail3.id, name, category_id
)
select table_temp.category_id,
       id,
       name,
       total_num,
       avg_category
from temp_table
         left join (
    select category_id,
           avg(total_num) as avg_category
    from temp_table
    group by category_id
) as table_temp
                   on temp_table.category_id = table_temp.category_id
where total_num > avg_category
order by id asc;

-- 11. 查询每个用户的注册日期、总登录次数以及在2021年的登录次数、订单数、订单总额

drop table if exists order_info3;

create table order_info3
(
    order_id     varchar(32) comment '订单id',
    user_id      varchar(32) comment '商品id',
    event_time   date comment '商品销售日期',
    total_amount int comment '订单总额',
    total_count  int comment '商品件数'
) comment '订单明细表3'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/order_info3';

INSERT INTO order_info3
VALUES ('301004', '2', '2021-09-30', 170, 1),
       ('301005', '4', '2021-10-01', 160, 1),
       ('301003', '1', '2021-10-02', 300, 2),
       ('301002', '2', '2021-10-03', 235, 2);

-- 用户登录明细表
select *
from user_login_detail;

-- 订单明细表
select *
from order_info3
order by user_id asc;

with temp_table as (
    select user_id,
           min(active_date)                               as register_date,
           count(distinct active_date)                    as login_nums,
           count(`if`(year(active_date) = 2021, 1, null)) as 2021_login_nums
    from (
             select user_id,
                    active_date
             from user_login_detail
             group by user_id, active_date
             union all
             select user_id,
                    event_time as active_date
             from order_info3
             group by user_id, event_time
             order by user_id asc, active_date asc
         ) as table_temp
    group by user_id
)
select temp_table.user_id,
       register_date,
       login_nums,
       nvl(2021_login_nums, 0)         as 2021_login_nums,
       nvl(2021_order_nums, 0)         as 2021_order_nums,
       nvl(2021_order_total_amount, 0) as 2021_order_total_amount
from temp_table
         left join (
    select user_id,
           sum(total_amount) as 2021_order_total_amount,
           sum(total_count)  as 2021_order_nums
    from order_info3
    where year(event_time) = 2021
    group by user_id
) as table_temp
                   on temp_table.user_id = table_temp.user_id
order by temp_table.user_id asc;


-- 12. 商品价格修改表 -> 请求出具体日期2022-04-20的全部商品价格表，假设所有商品在修改之前价格默认是99

drop table if exists price_modification_details;

create table price_modification_details
(
    id               varchar(32) comment '商品id',
    new_price        int comment '新的价格',
    changeprice_date date comment '修改价格的日期'
) comment '价格更改明细表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/price_modification_details';

INSERT INTO price_modification_details
VALUES ('1', 20, '2022-04-10'),
       ('1', 30, '2022-04-12'),
       ('1', 40, '2022-04-20'),
       ('1', 80, '2022-04-21'),
       ('2', 55, '2022-04-19'),
       ('2', 65, '2022-04-21'),
       ('3', 45, '2022-04-21');

-- 商品属性表
select *
from product_attr;

-- 价格修改明细表
select *
from price_modification_details;


select name,
       nvl(new_price, 99) as new_price
from product_attr
         left join (
    select id, new_price
    from (
             select id,
                    new_price,
                    changeprice_date,
                    rank() over (partition by id order by changeprice_date desc) as ranking
             from price_modification_details
             where changeprice_date <= '2022-04-20'
         ) as table_temp
    where ranking = 1
) as table_temp
                   on product_attr.id = table_temp.id
order by name asc;

-- 13. 求用户首单中及时订单的比例，保留两位小数

drop table if exists delivery_info;

create table delivery_info
(
    delivery_id varchar(32) comment '配送订单id',
    user_id     varchar(32) comment '用户id',
    order_date  date comment '下单日期',
    custom_date date comment '顾客希望的配送日期'
) comment '快餐配送信息表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/delivery_info';

INSERT INTO delivery_info
VALUES ('1', '1', '2021-08-01', '2021-08-02'),
       ('2', '2', '2021-08-02', '2021-08-02'),
       ('3', '1', '2021-08-11', '2021-08-12'),
       ('4', '3', '2021-08-24', '2021-08-24'),
       ('5', '3', '2021-08-21', '2021-08-22'),
       ('6', '2', '2021-08-11', '2021-08-13'),
       ('7', '4', '2021-08-09', '2021-08-09');

-- 配送信息表
select *
from delivery_info;

with temp_table as (
    select delivery_info.user_id,
           order_date,
           custom_date,
           1 as flag
    from delivery_info
             join (
        select user_id,
               min(order_date) as first_order
        from delivery_info
        group by user_id
    ) as table_temp
                  on delivery_info.user_id = table_temp.user_id and order_date = first_order
)
select round((count(`if`(order_date = custom_date, 1, null)) / count(distinct user_id)) * 100, 2) as rate
from temp_table
group by flag;

-- 14. 向用户1推荐它的朋友收藏的商品，但是不能包括用户1已经收藏过的商品

drop table if exists friendship;

create table friendship
(
    user1_id varchar(32) comment '用户1 id',
    user2_id varchar(32) comment '用户2 id'
) comment '用户关系表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/friendship';

drop table if exists user_favorites;

create table user_favorites
(
    user_id varchar(32) comment '用户id',
    id      varchar(32) comment '商品id'
) comment '用户收藏表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/user_favorites';

INSERT INTO friendship
VALUES ('1', '2'),
       ('1', '3'),
       ('1', '4'),
       ('2', '3'),
       ('2', '4'),
       ('2', '5'),
       ('6', '1');

INSERT INTO user_favorites
VALUES ('1', '88\r'),
       ('2', '23\r'),
       ('3', '24\r'),
       ('4', '56\r'),
       ('5', '11\r'),
       ('6', '23\r'),
       ('2', '77\r'),
       ('3', '77\r'),
       ('6', '88\r');

-- 用户关系表
select *
from friendship;

-- 用户收藏表
select *
from user_favorites;


select distinct id
from user_favorites
where user_id in (
    select user2_id
    from (
             select user1_id,
                    user2_id
             from friendship
             union all
             select user2_id as user1_id,
                    user1_id as user2_id
             from friendship
         ) as table_temp
    where user1_id = '1'
)
  and id not in (
    select id
    from user_favorites
    where user_id = '1'
)
order by id asc;


-- 15. 查询所有用户的大于等于两天的连续登录区间

drop table if exists user_active_2;

create table user_active_2
(
    user_id     varchar(32) comment '用户id',
    active_date date comment '用户登录日期时间'
) comment '用户活跃表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/user_active_2';

INSERT INTO user_active_2
VALUES ('1', '2021-04-21'),
       ('1', '2021-04-22'),
       ('1', '2021-04-23'),
       ('1', '2021-04-25'),
       ('1', '2021-04-26'),
       ('1', '2021-04-28'),
       ('2', '2021-04-23'),
       ('2', '2021-04-24'),
       ('2', '2021-04-25'),
       ('3', '2021-04-23');

-- 用户活跃表2
select *
from user_active_2;

with temp_table as (
    select user_id,
           active_date,
           ranking,
           date_sub(active_date, ranking) as sub_date
    from (
             select user_id,
                    active_date,
                    rank() over (partition by user_id order by active_date asc) as ranking
             from user_active_2
         ) as table_temp
    order by user_id asc, active_date asc
)
select user_id,
       login_interval
from (
         select user_id,
                sub_date,
                concat(`if`(min(active_date) = max(active_date), '', min(active_date)), ' ~ ',
                       `if`(min(active_date) = max(active_date), '', max(active_date))) as login_interval
         from temp_table
         group by user_id, sub_date
     ) as table_temp
where login_interval != ' ~ ';

-- 16. 求男性和女性每日购物总金额，若当天没有购物，统计结果为0

drop table if exists user_info;

create table user_info
(
    user_id varchar(32) comment '用户id',
    sex     varchar(32) comment '用户性别'
) comment '用户信息表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/user_info';

drop table if exists order_detail_4;

create table order_detail_4
(
    order_id  varchar(32) comment '订单id',
    id        varchar(32) comment '商品id',
    price     int comment '订单总额',
    num       int comment '商品件数',
    sale_date date comment '商品销售日期',
    user_id   varchar(32) comment '用户id'
) comment '订单明细表4'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/order_detail_4';

INSERT INTO user_info
VALUES ('1', '男'),
       ('2', '男'),
       ('3', '男'),
       ('4', '男'),
       ('5', '女'),
       ('6', '女'),
       ('7', '女'),
       ('8', '女');

INSERT INTO order_detail_4
VALUES ('1', '2', 500, 3, '2022-01-02', '1'),
       ('2', '2', 800, 3, '2022-01-03', '2'),
       ('3', '2', 1200, 3, '2022-01-03', '3'),
       ('4', '2', 200, 3, '2022-01-04', '2'),
       ('5', '2', 700, 3, '2022-01-04', '4'),
       ('6', '2', 300, 3, '2022-01-05', '1'),
       ('7', '2', 430, 3, '2022-01-06', '2'),
       ('8', '2', 230, 3, '2022-01-08', '3'),
       ('9', '2', 320, 3, '2022-01-02', '5'),
       ('10', '2', 590, 3, '2022-01-03', '6'),
       ('11', '2', 100, 3, '2022-01-04', '8'),
       ('12', '2', 40, 3, '2022-01-06', '7'),
       ('13', '2', 20, 3, '2022-01-07', '2');

-- 用户信息表
select *
from user_info;

-- 订单明细表
select *
from order_detail_4;

with temp_table as (
    select sex,
           sale_date,
           sum(price) as total_price
    from order_detail_4
             left join user_info
                       on order_detail_4.user_id = user_info.user_id
    group by sex, sale_date
    order by sale_date asc, sex asc
)
select sale_date,
       case
           when sum(`if`(sex = '男', total_price, 0)) is not null then sum(`if`(sex = '男', total_price, 0))
           else 0
           end as `男`,
       case
           when sum(`if`(sex = '女', total_price, 0)) is not null then sum(`if`(sex = '女', total_price, 0))
           else 0
           end as `女`
from temp_table
group by sale_date
order by sale_date asc;

-- 17. 查询该日期加前六天（7天内）消费者的订单总额平均值，保留两位小数，按照日期升序排序

drop table if exists order_detail_5;

create table order_detail_5
(
    order_id  varchar(32) comment '订单id',
    price     int comment '订单总额',
    sale_date date comment '商品销售日期'
) comment '订单明细表5'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/order_detail_5';

INSERT INTO order_detail_5
VALUES ('1', 1500, '2022-01-01'),
       ('2', 2000, '2022-01-02'),
       ('3', 5000, '2022-01-02'),
       ('4', 6000, '2022-01-03'),
       ('5', 2000, '2022-01-04'),
       ('6', 2500, '2022-01-05'),
       ('7', 1000, '2022-01-06'),
       ('8', 500, '2022-01-07'),
       ('9', 300, '2022-01-07'),
       ('10', 200, '2022-01-07'),
       ('11', 3000, '2022-01-08'),
       ('12', 10000, '2022-01-09'),
       ('13', 8000, '2022-01-10'),
       ('14', 2000, '2022-01-10');

-- 订单消费明细表
select *
from order_detail_5;

with temp_table as (
    select sale_date,
           sum(price) as total_price
    from order_detail_5
    group by sale_date
)
select sale_date,
       round(avg_total_price, 2) as avg_total_price
from (
         select sale_date,
                avg(total_price)
                    over (order by sale_date asc rows between 6 preceding and current row) as avg_total_price
         from temp_table
     ) as table_temp

where sale_date >= '2022-01-07'
order by sale_date asc;

-- 18. 购买过商品1和商品2但是没有购买商品3的顾客
drop table if exists order_detail_6;

create table order_detail_6
(
    order_id  varchar(32) comment '订单id',
    id        varchar(32) comment '商品id',
    price     int comment '订单总额',
    num       int comment '商品件数',
    sale_time date comment '商品销售日期',
    user_id   varchar(32) comment '用户id'
) comment '订单明细表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/order_detail_6';

INSERT INTO order_detail_6
VALUES ('1001', '1', 100, 10, '2022-04-01', '1'),
       ('1002', '2', 100, 10, '2022-04-01', '1'),
       ('1003', '3', 100, 10, '2022-04-02', '1'),
       ('1004', '1', 100, 10, '2022-04-02', '2'),
       ('1005', '2', 100, 10, '2022-04-03', '2'),
       ('1006', '1', 100, 10, '2022-04-03', '3'),
       ('1007', '2', 100, 10, '2022-04-04', '4'),
       ('1008', '4', 100, 10, '2022-04-04', '4'),
       ('1009', '1', 100, 10, '2022-04-05', '5'),
       ('1010', '4', 100, 10, '2022-04-06', '5');

-- 订单明细表
select *
from order_detail_6;

select user_id
from order_detail_6
where id <= 2
group by user_id
having count(distinct id) = 2
   and user_id not in (
    select user_id
    from order_detail_6
    where id = 3
)
order by user_id asc;

-- 19. 求每日商品1销量减商品2销量的差值，如果当天没有该商品的销售记录，那么销量就为0

drop table if exists order_detail_7;

create table order_detail_7
(
    order_id  varchar(32) comment '订单id',
    id        varchar(32) comment '商品id',
    price     int comment '订单总额',
    num       int comment '商品件数',
    sale_date date comment '商品销售日期',
    user_id   varchar(32) comment '用户id'
) comment '订单明细表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/order_detail_7';

INSERT INTO order_detail_7
VALUES ('1001', '1', 100, 8, '2022-04-01', '1'),
       ('1002', '2', 100, 18, '2022-04-01', '1'),
       ('1003', '1', 100, 19, '2022-04-02', '1'),
       ('1004', '2', 100, 7, '2022-04-02', '2'),
       ('1005', '1', 100, 24, '2022-04-03', '2'),
       ('1006', '2', 100, 10, '2022-04-03', '3'),
       ('1007', '1', 100, 9, '2022-04-04', '4'),
       ('1008', '2', 100, 10, '2022-04-04', '4'),
       ('1009', '1', 100, 8, '2022-04-05', '5'),
       ('1010', '2', 100, 10, '2022-04-06', '5');

-- 订单明细表
select *
from order_detail_7;

select distinct nvl(temp_1.sale_date, temp_2.sale_date) as sale_date,
                nvl(temp_1.num, 0) - nvl(temp_2.num, 0) as sub_value
from (
         select id,
                num,
                sale_date
         from order_detail_7
         where id = '1'
     ) as temp_1
         full outer join (
    select id,
           num,
           sale_date
    from order_detail_7
    where id = '2'
) as temp_2
                         on temp_1.sale_date = temp_2.sale_date
order by sale_date asc;

-- 20. 查出用户最近的三笔订单，假设每个用户每天只有一笔订单，如果用户总订单小于3，则输出用户的全部订单。按照用户id升序排序，每个用户的三笔订单按照日期升序排序。

drop table if exists order_detail_8;

create table order_detail_8
(
    order_id  varchar(32) comment '订单id',
    id        varchar(32) comment '商品id',
    price     int comment '订单总额',
    num       int comment '商品件数',
    sale_date date comment '商品销售日期',
    user_id   varchar(32) comment '用户id'
) comment '订单明细表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/order_detail_8';

INSERT INTO order_detail_8
VALUES ('1001', '1', 100, 8, '2021-02-01', '1'),
       ('1002', '2', 100, 18, '2022-04-01', '1'),
       ('1003', '1', 100, 19, '2022-04-03', '1'),
       ('1004', '2', 100, 7, '2022-04-02', '2'),
       ('1005', '1', 100, 24, '2022-04-03', '2'),
       ('1006', '2', 100, 10, '2022-04-03', '3'),
       ('1007', '1', 100, 9, '2022-04-04', '3'),
       ('1008', '2', 100, 10, '2022-04-05', '3'),
       ('1009', '1', 100, 8, '2022-04-08', '3'),
       ('1010', '2', 100, 10, '2022-04-06', '4');

-- 订单明细表
select *
from order_detail_8;

select user_id, sale_date, order_id, id, price, num
from (
         select *,
                rank() over (partition by user_id order by sale_date desc) as ranking
         from order_detail_8
     ) as table_temp
where ranking <= 3
order by user_id asc, sale_date asc;

-- 21. 用户登录日期的最大空挡期：对于每一个用户，求出每次访问和下一次访问之间的最大空档天数，如果是表中的最后一次访问，
-- 则需要计算最后一次访问和今天之间的天数，假设今天：2022-01-01。

drop table if exists user_active_3;

create table user_active_3
(
    user_id     varchar(32) comment '用户id',
    active_date date comment '用户登录日期时间'
) comment '用户活跃表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/dsc/rds/user_active_3';

INSERT INTO user_active_3
VALUES ('1', '2021-10-20'),
       ('1', '2021-11-28'),
       ('1', '2021-12-08'),
       ('2', '2021-10-05'),
       ('2', '2021-12-09'),
       ('3', '2021-11-11');

-- 用户活跃表
select *
from user_active_3;

select user_id,
       max(gap_period) as max_gap_period
from (
         select user_id,
                datediff(lead(active_date, 1, '2022-01-01') over (partition by user_id order by active_date asc),
                         active_date) as gap_period
         from user_active_3
     ) as table_temp
group by user_id
order by user_id asc;

-- 22. 账号多地登录

drop table if exists user_active_4;

create table user_active_4
(
    user_id    varchar(32) comment '用户id',
    ip_address varchar(32) comment '用户登录ip地址',
    login_ts   timestamp not null comment '登录时间',
    logout_ts  timestamp not null comment '登出时间'
) comment '用户活跃表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/user_active_4';

INSERT INTO user_active_4
VALUES ('1', '1', '2021-02-01 01:00:00', '2021-02-01 01:30:00'),
       ('1', '2', '2021-02-01 00:00:00', '2021-02-01 03:30:00'),
       ('2', '6', '2021-02-01 12:30:00', '2021-02-01 14:00:00'),
       ('2', '7', '2021-02-02 12:30:00', '2021-02-02 14:00:00'),
       ('3', '9', '2021-02-01 08:00:00', '2021-02-01 08:59:59'),
       ('3', '13', '2021-02-01 09:00:00', '2021-02-01 09:59:59'),
       ('4', '10', '2021-02-01 08:00:00', '2021-02-01 09:00:00'),
       ('4', '11', '2021-02-01 09:00:00', '2021-02-01 09:59:59');

-- 用户活跃表
select *
from user_active_4;

select u1.user_id, u1.ip_address as ip_1, u2.ip_address as ip_2
from user_active_4 u1,
     user_active_4 u2
where u1.user_id = u2.user_id
  and u1.ip_address < u2.ip_address
  and (u1.login_ts between u2.login_ts and u2.logout_ts or u1.logout_ts between u2.login_ts and u2.logout_ts)
order by u1.user_id asc, ip_1 asc;

-- 23. 销售额完成任务指标的商品
--     -> 假如每个商品每个月需要售卖出一定的销售总额，请查询连续两个月销售总额大于任务总额的商品

drop table if exists order_info;

create table order_info
(
    order_id  varchar(32) comment '订单id',
    id        varchar(32) comment '商品id',
    price     int comment '订单总额',
    num       int comment '商品件数',
    sale_date date comment '商品销售日期'
) comment '订单详情表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/order_info';

drop table if exists product_supply;

create table product_supply
(
    id         varchar(32) comment '商品id',
    assignment int comment '商品每个月固定的销售任务指标'
) comment '商品供应数量表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/product_supply';

INSERT INTO order_info
VALUES ('2', '1', 107100, 1, '2021-06-02'),
       ('4', '2', 10400, 1, '2021-06-20'),
       ('11', '2', 58800, 1, '2021-07-23'),
       ('1', '2', 49300, 1, '2021-05-03'),
       ('15', '1', 75500, 1, '2021-05-23'),
       ('10', '1', 102100, 1, '2021-06-15'),
       ('14', '2', 56300, 1, '2021-07-21'),
       ('19', '2', 101100, 1, '2021-05-09'),
       ('8', '1', 64900, 1, '2021-07-26'),
       ('7', '1', 90900, 1, '2021-06-14');

INSERT INTO product_supply
VALUES ('1', 21000),
       ('2', 10400);

-- 订单详情表
select *
from order_info;

-- 商品供应数量表
select *
from product_supply;

with temp_table as (
    select o.id,
           date_format(sale_date, 'yyyy-MM') as year_month,
           sum(price)                        as total_price,
           assignment
    from order_info o
             left join product_supply p
                       on o.id = p.id
    group by assignment, o.id, date_format(sale_date, 'yyyy-MM')
)
select id, year_month, next_year_month
from (
         select id,
                year_month,
                lead(year_month, 1, '9999-12') over (partition by id order by year_month asc) as next_year_month
         from temp_table
         where total_price > assignment
     ) as table_temp
where substr(year_month, 1, 4) = substr(next_year_month, 1, 4)
  and cast(substr(year_month, 6, 2) as int) = cast(substr(next_year_month, 6, 2) as int) - 1;

-- 24. 按照销售件数对商品进行分类
--     ->   冷门商品 0  ->  一般商品 5001   ->  热门商品 20000

drop table if exists sales_num;

create table sales_num
(
    id  varchar(32) comment '商品id',
    num int comment '商品销售件数'
) comment '商品销售详情表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/sales_num';

INSERT INTO sales_num
VALUES ('1', 300),
       ('2', 4000),
       ('3', 9000),
       ('4', 40000);

-- 商品销售详情表
select *
from sales_num;

with temp_table as (
    select id,
           case
               when num >= 20000 then '热门商品'
               when num >= 5001 then '一般商品'
               else '冷门商品'
               end as category
    from sales_num
)
select temp_1.category,
       num
from (
         select '冷门商品' as category
         union all
         select '一般商品' as category
         union all
         select '热门商品' as category
     ) as temp_1
         left join (
    select category,
           count(distinct id) as num
    from temp_table
    group by category
) as temp_2
                   on temp_1.category = temp_2.category;

-- 25.