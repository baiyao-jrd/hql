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

-- 25. 付款率：用户下单之后需要付款，如果在30min内未付款，就会超时。求每个用户的付款率。

drop table if exists payment_detail;

create table payment_detail
(
    user_id     varchar(32) comment '用户id',
    `timestamp` timestamp not null comment '用户下单时间',
    action      varchar(32) comment '下单是否超时'
) comment '用户付款详情表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/payment_detail';

drop table if exists user_info_2;

create table user_info_2
(
    user_id     varchar(32) comment '用户id',
    active_date date comment '用户注册日期'
) comment '用户信息表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/user_info_2';

set hive.execution.engine = spark;

INSERT INTO payment_detail
VALUES ('1', '2021-01-05 19:30:46', 'timeout'),
       ('1', '2021-07-14 06:00:00', 'timeout'),
       ('2', '2021-01-05 19:30:46', 'success'),
       ('2', '2021-07-14 06:00:00', 'success'),
       ('2', '2021-01-04 19:30:46', 'success'),
       ('3', '2021-07-14 06:00:00', 'success'),
       ('3', '2021-01-05 19:30:46', 'timeout');

INSERT INTO user_info_2
VALUES ('1', '2021-10-20'),
       ('2', '2021-10-05'),
       ('3', '2021-11-11'),
       ('4', '2022-04-12');

-- 用户付款详情表
select *
from payment_detail;

-- 用户信息表
select *
from user_info_2;

select user_info_2.user_id,
       nvl(payment_rate, 0) as payment_rate
from user_info_2
         left join (
    select user_id,
           count(`if`(action = 'success', 1, null)) / count(`timestamp`) as payment_rate
    from payment_detail
    group by user_id
) as table_temp
                   on user_info_2.user_id = table_temp.user_id;

-- 26. 商品库存变化

drop table if exists stock_detail;

create table stock_detail
(
    id     varchar(32) comment '商品id',
    `date` date comment '变化时间',
    action varchar(32) comment '补货或者是售货',
    amount int comment '补货或者售货数量'
) comment '商品库存明细表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/stock_detail';

INSERT INTO stock_detail
VALUES ('1', '2021-01-01', 'supply', 2000),
       ('1', '2021-01-03', 'sell', 1000),
       ('1', '2021-01-05', 'supply', 3000),
       ('2', '2021-01-01', 'supply', 7000),
       ('2', '2021-01-01', 'supply', 1000),
       ('2', '2021-01-04', 'sell', 8000);

-- 商品库存明细表
select *
from stock_detail;

with temp_table as (
    select id,
           `date`,
           action,
           case
               when action = 'supply' then sum(amount)
               when action = 'sell' then -sum(amount)
               end as amount_total
    from stock_detail
    group by id, `date`, action
)
select id,
       `date`,
       sum(amount_total) over (partition by id order by `date` asc) as stock
from temp_table
order by id asc, `date` asc;

-- 27. 各品类销量前三的所有商品：从商品销售明细表中查询各个品类销售数量前三的商品
--     如果该品类小于三个商品，则输出所有的商品销量。

drop table if exists order_summary_1;

create table order_summary_1
(
    id          varchar(32) comment '商品id',
    category_id varchar(32) comment '商品所属品类id',
    sum         int comment '商品销售总额累计'
) comment '订单汇总表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/order_summary_1';

INSERT INTO order_summary_1
VALUES ('1', '1', 66),
       ('2', '1', 23),
       ('3', '1', 78),
       ('4', '2', 23),
       ('5', '3', 89),
       ('6', '1', 99),
       ('9', '1', 128);

-- 订单汇总表
select *
from order_summary_1;

-- 品类表
select *
from category;

with temp_table as (
    select category_name,
           id,
           rank() over (partition by category_name order by sum desc) as ranking
    from order_summary_1 o
             left join category c on o.category_id = c.category_id
)
select distinct id, category_name, ranking
from temp_table
where ranking <= 3
order by category_name asc, id asc;

-- 28. 各品类中商品价格的中位数：如果是偶数就输出中间两个值的平均值，如果是奇数就输出中间数即可。

drop table if exists product_detail;

create table product_detail
(
    id          varchar(32) comment '商品id',
    category_id varchar(32) comment '商品所属品类id',
    price       int comment '商品售价'
) comment '商品详情表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/product_detail';

INSERT INTO product_detail
VALUES ('1', '1', 23),
       ('2', '1', 45),
       ('3', '1', 46),
       ('4', '2', 56),
       ('5', '2', 45),
       ('6', '3', 76),
       ('7', '3', 23),
       ('8', '3', 55);

-- 商品详情表
select *
from product_detail;

-- 品类表
select *
from category;


with temp_table as (
    select category_name,
           id,
           price,
           row_number() over (partition by category_name order by price asc) as rn,
           count(id) over (partition by category_name)                       as total_product_nums
    from product_detail p
             left join category c
                       on p.category_id = c.category_id
)
select category_name,
       round((max(price) + min(price)) / 2, 2) as median
from (
         select category_name,
                id,
                price
         from temp_table
         where rn = `if`(total_product_nums % 2 = 0, total_product_nums / 2, `ceiling`(total_product_nums / 2))
            or rn = `if`(total_product_nums % 2 = 0, total_product_nums / 2 + 1, `ceiling`(total_product_nums / 2))
     ) as table_temp
group by category_name
order by category_name asc;


-- 29. 找出销售额连续多天超过100的记录

drop table if exists order_detail_9;

create table order_detail_9
(
    order_id varchar(32) comment '商品id',
    `date`   date comment '商品销售日期',
    price    int comment '商品当天销售额'
) comment '订单明细表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/order_detail_9';

INSERT INTO order_detail_9
VALUES ('1', '2021-01-01', 10),
       ('2', '2021-01-02', 109),
       ('3', '2021-01-03', 158),
       ('4', '2021-01-04', 99),
       ('5', '2021-01-05', 145),
       ('6', '2021-01-06', 1455),
       ('7', '2021-01-07', 1199),
       ('8', '2021-01-08', 188);

-- 订单明细表
select *
from order_detail_9;

with temp_table as (
    select order_id,
           `date`,
           price,
           ranking,
           flag,
           date_sub(`date`, ranking) as date_sub
    from (
             select order_id,
                    `date`,
                    price,
                    rank() over (partition by flag order by `date` asc) as ranking,
                    flag
             from (select *, 1 as flag from order_detail_9) as order_detail_9
             where price > 100
         ) as table_temp
)
select order_id,
       `date`,
       price
from temp_table
where date_sub in (select date_sub
                   from temp_table
                   group by date_sub
                   having count(*) >= 3)
order by `date` asc;

set hive.execution.engine = spark;

-- 30. 查询有新注册用户的当天的新用户数量、新用户的第一天留存率

drop table if exists user_login_detail_1;

create table user_login_detail_1
(
    user_id varchar(32) comment '用户id',
    `date`  date comment '商品销售日期',
    price   int comment '商品当天销售额'
) comment '用户登录明细表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/user_login_detail_1';

INSERT INTO user_login_detail_1
VALUES ('1', '2022-01-01', 50),
       ('1', '2022-01-02', 100),
       ('2', '2022-03-01', 100),
       ('3', '2022-01-01', 100),
       ('3', '2022-02-01', 800);

-- 用户登录明细表
select *
from user_login_detail_1;

with temp_table as (
    select user_id,
           min(`date`) as register_date
    from user_login_detail_1
    group by user_id
)
select temp_1.register_date,
       new_user_nums,
       round(retention_user_nums / new_user_nums, 2) as ratention_rate
from (
         select register_date,
                count(distinct user_id) as new_user_nums
         from temp_table
         group by register_date
     ) as temp_1
         left join (
    select register_date,
           count(`if`(ratention_user = 1, 1, null)) as retention_user_nums
    from (
             select user_id,
                    register_date,
                    case
                        when date_add(register_date, 1) in
                             (select `date` from user_login_detail_1 where user_id = t.user_id) then 1
                        else 0
                        end as ratention_user
             from temp_table t
         ) as temp
    group by register_date
) temp_2
                   on temp_1.register_date = temp_2.register_date
order by temp_1.register_date asc;

-- 31. 某商品售卖明细表求出连续售卖的时间区间和非连续售卖的时间区间
--     只统计2021-01-01到2021-12-31之间的数据，如果有非售卖记录，那就是nosale的起止日期
--     如果有售卖记录，那就是sale的起止日期

drop table if exists sales;

create table sales
(
    `date` date comment '商品销售日期'
) comment '售卖表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/sales';

drop table if exists no_sales;

create table no_sales
(
    `date` date comment '商品销售日期'
) comment '无售卖表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/no_sales';

INSERT INTO sales
VALUES ('2020-12-30'),
       ('2020-12-31'),
       ('2021-01-04'),
       ('2021-01-05');

INSERT INTO no_sales
VALUES ('2020-12-29'),
       ('2021-01-01'),
       ('2021-01-02'),
       ('2021-01-03'),
       ('2021-01-06');

-- 售卖表
select *
from sales;

-- 无售卖表
select *
from no_sales;

select concat('售卖区间：', min(`date`), ' ~ ', max(`date`)) as area
from (
         select `date`, date_sub(`date`, rk) as rk_base
         from (
                  select `date`,
                         rank() over (partition by flag order by `date`) as rk
                  from (select `date`, 1 as flag from sales) as temp_1
              ) as temp_2
     ) as temp_3
where `date` between '2021-01-01' and '2021-12-31'
group by rk_base
union all
select concat('非售卖区间：', min(`date`), ' ~ ', max(`date`)) as area
from (
         select `date`, date_sub(`date`, rk) as rk_base
         from (
                  select `date`,
                         rank() over (partition by flag order by `date`) as rk
                  from (select `date`, 1 as flag from no_sales) as temp_1
              ) as temp_2
     ) as temp_3
where `date` between '2021-01-01' and '2021-12-31'
group by rk_base
order by substr(area, -1, 23) asc;

-- 32. 有登录记录和交易记录两张表，查询 -> 多少用户登录了但未交易 -> 多少用户登录了并进行一次、两次，，，，交易

drop table if exists register;

create table register
(
    user_id varchar(32) comment '用户id',
    `date`  date comment '用户登录日期'
) comment '登录记录表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/register';

drop table if exists transaction_recode;

create table transaction_recode
(
    user_id varchar(32) comment '用户id',
    `date`  date comment '商品销售日期',
    price   int comment '商品当天销售额'
) comment '交易记录表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/transaction_recode';

INSERT INTO register
VALUES ('1', '2021-01-01'),
       ('2', '2021-01-02'),
       ('3', '2021-01-01'),
       ('6', '2021-01-03'),
       ('1', '2021-01-02'),
       ('2', '2021-01-03'),
       ('1', '2021-01-04'),
       ('7', '2021-01-11'),
       ('9', '2021-01-25'),
       ('8', '2021-01-28');

INSERT INTO transaction_recode
VALUES ('1', '2021-01-02', 120),
       ('2', '2021-01-03', 120),
       ('7', '2021-01-11', 120),
       ('1', '2021-01-04', 120),
       ('9', '2021-01-25', 120),
       ('9', '2021-01-25', 120),
       ('8', '2021-01-28', 120),
       ('9', '2021-01-25', 120);

-- 登录记录表
select *
from register;

-- 交易记录表
select *
from transaction_recode;

-- 方式1：按照用户统计
with temp_table as (
    select user_id,
           count(*) as transa_nums
    from transaction_recode
    group by user_id)
select case
           when transa_nums = 0 then concat(user_nums, '人登录了未交易')
           else concat(user_nums, '人登录了，并进行了', transa_nums, '次交易')
           end as report
from (
         select transa_nums,
                count(user_id) as user_nums
         from (
                  select r.user_id,
                         nvl(transa_nums, 0) as transa_nums
                  from (select user_id from register group by user_id) as r
                           left join temp_table t
                                     on r.user_id = t.user_id
              ) as table_temp
         group by transa_nums
     ) as table_temp;


-- 方式2：按照人次统计
with temp_table as (
    select transa_nums,
           count(user_id) as user_nums,
           1              as flag
    from (
             select r.user_id,
                    r.`date`,
                    nvl(transa_nums, 0) as transa_nums
             from register r
                      left join (select user_id,
                                        `date`,
                                        count(*) as transa_nums
                                 from transaction_recode
                                 group by user_id, `date`) as t
                                on r.user_id = t.user_id and r.`date` = t.`date`
         ) as table_temp
    group by transa_nums
)
select temp_1.transa_nums as transa_nums,
       nvl(user_nums, 0)  as user_nums
from temp_table as temp_2
         right join (select 0 as transa_nums
                     union
                     select row_number() over () as transa_nums
                     from transaction_recode) as temp_1
                    on temp_1.transa_nums = temp_2.transa_nums
where temp_1.transa_nums <= (
    select max(transa_nums)
    from temp_table
    group by flag
)
order by temp_1.transa_nums asc;

-- 33. 按年度列出销售总额

drop table if exists product_detail_2;

create table product_detail_2
(
    product_id   varchar(32) comment '商品id',
    product_name varchar(32) comment '商品名称'
) comment '产品明细表'
    row format delimited fields terminated by '/t'
        null defined as ''
    location '/warehouse/sdc/rds/product_detail_2';

drop table if exists deal_record;

create table deal_record
(
    product_id varchar(32) comment '商品id',
    start_date date comment '商品销售起始日期',
    end_date   date comment '商品销售结束日期',
    avg        int comment '商品平均每日销售额'
) comment '成交记录表'
    row format delimited fields terminated by '/t'
        null defined as ''
    location '/warehouse/sdc/rds/deal_record';

INSERT INTO product_detail_2
VALUES ('1', 'xiaomi'),
       ('2', 'apple'),
       ('3', 'vivo');

INSERT INTO deal_record
VALUES ('1', '2019-01-25', '2019-02-28', 100),
       ('2', '2018-12-01', '2020-01-01', 10),
       ('3', '2019-12-01', '2020-01-31', 1);

-- 产品明细表
select *
from product_detail_2;

-- 成交记录表
select *
from deal_record;

select product_name,
       start_date,
       end_date,
       avg
from deal_record d
         left join product_detail_2 p on d.product_id = p.product_id;


with temp_table as (
    select min(year_date) as min_year,
           max(year_date) as max_year
    from (
             select year(start_date) as year_date,
                    1                as flag
             from deal_record
             union all
             select year(end_date) as year_date,
                    1              as flag
             from deal_record
         ) as table_temp
    group by flag
)
select product_name,
       year(date_flag)                                 as year,
       datediff(next_date_flag, date_flag) * avg + avg as total_sales
from (
         select product_id,
                date_flag,
                lead(date_flag, 1, concat(year(date_flag), '-01-01'))
                     over (partition by product_id order by date_flag) as next_date_flag,
                avg
         from (
                  select product_id, date_flag, avg
                  from (
                           select product_id, concat(seq, '-', month_day) as date_flag, avg
                           from (
                                    select seq
                                    from (
                                             select posexplode(split(space(max_year), ' ')) as (seq, dummy)
                                             from temp_table
                                         ) as temp1
                                             left join temp_table
                                    where seq between min_year and max_year
                                ) as temp_1,
                                (select '01-01' as month_day union all select '12-31' as month_day) as temp_2,
                                (select product_id, avg from deal_record) as temp_3
                           union
                           select product_id, cast(start_date as string) as date_flag, avg
                           from deal_record
                           union all
                           select product_id, cast(end_date as string) as date_flag, avg
                           from deal_record
                       ) as temp_4
                  where date_flag between (select start_date
                                           from deal_record
                                           where product_id = temp_4.product_id) and (select end_date from deal_record where product_id = temp_4.product_id)
                  group by product_id, date_flag, avg
              ) as temp_5
     ) as temp_6
         left join product_detail_2
                   on temp_6.product_id = product_detail_2.product_id
where substr(date_flag, 1, 4) = substr(next_date_flag, 1, 4)
  and next_date_flag >= date_flag;

-- 34. 查询周内每天每个商品类别售卖了多少件

drop table if exists order_detail_2;

create table order_detail_2
(
    order_id  varchar(32) not null comment '订单id',
    id        varchar(32) comment '商品id',
    price     int comment '订单总额',
    num       int comment '商品件数',
    sale_date date comment '商品销售日期'
) comment '订单明细表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/order_detail_2';

drop table if exists product_detail2;

create table product_detail2
(
    product_id   varchar(32) comment '商品id',
    product_name varchar(32) comment '商品名称',
    category     varchar(32) comment '商品类别'
) comment '产品明细表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/product_detail2';

INSERT INTO order_detail_2
VALUES ('1', '1', 80, 8, '2021-12-29'),
       ('2', '1', 10, 1, '2021-12-30'),
       ('3', '2', 55, 5, '2021-04-30'),
       ('3', '2', 55, 5, '2021-04-30'),
       ('4', '3', 550, 10, '2021-03-31'),
       ('5', '4', 550, 15, '2021-05-04'),
       ('6', '2', 30, 3, '2021-08-07'),
       ('7', '2', 60, 6, '2020-08-09'),
       ('8', '4', 550, 15, '2021-05-05');

INSERT INTO product_detail2
VALUES ('1', 'bingxiang', 'dianqi'),
       ('2', 'xiyiji', 'dianqi'),
       ('3', 'xiaomi', 'phone'),
       ('4', 'apple', 'phone'),
       ('5', 'dami', 'food'),
       ('6', 'kuzi', 'cloth');

-- 订单明细表
select *
from order_detail_2;

-- 产品明细表
select *
from product_detail2;

with temp_table as (
    select day_of_week,
           category,
           sum(num) as total_nums
    from (
             select order_id,
                    category,
                    num,
                    case `dayofweek`(sale_date)
                        when 1 then '周日'
                        when 2 then '周一'
                        when 3 then '周二'
                        when 4 then '周三'
                        when 5 then '周四'
                        when 6 then '周五'
                        when 7 then '周六'
                        end as day_of_week
             from order_detail_2,
                  product_detail2
             where id = product_id
             group by order_id, category, num, sale_date
         ) as temp_1
    group by day_of_week, category
)
select category,
       sum(`if`(day_of_week = '周一', total_nums, 0)) as `周一`,
       sum(`if`(day_of_week = '周二', total_nums, 0)) as `周二`,
       sum(`if`(day_of_week = '周三', total_nums, 0)) as `周三`,
       sum(`if`(day_of_week = '周四', total_nums, 0)) as `周四`,
       sum(`if`(day_of_week = '周五', total_nums, 0)) as `周五`,
       sum(`if`(day_of_week = '周六', total_nums, 0)) as `周六`,
       sum(`if`(day_of_week = '周日', total_nums, 0)) as `周日`
from (
         select table_temp.category,
                nvl(day_of_week, null) as day_of_week,
                nvl(total_nums, 0)     as total_nums
         from (
                  select category
                  from product_detail2
                  group by category
              ) table_temp
                  left join temp_table
                            on table_temp.category = temp_table.category
     ) as table_temp
group by category
order by category asc;

-- dayofweek -> 1 周日 -> 7 周六
select `dayofweek`(`current_date`()) as test;

select b.category,
       nvl(sum(case when dayofweek(a.sale_date) = 2 then a.num end), 0) Monday,
       nvl(sum(case when dayofweek(a.sale_date) = 3 then a.num end), 0) Tuesday,
       nvl(sum(case when dayofweek(a.sale_date) = 4 then a.num end), 0) Wednesday,
       nvl(sum(case when dayofweek(a.sale_date) = 5 then a.num end), 0) Thursday,
       nvl(sum(case when dayofweek(a.sale_date) = 6 then a.num end), 0) Friday,
       nvl(sum(case when dayofweek(a.sale_date) = 7 then a.num end), 0) Saturday,
       nvl(sum(case when dayofweek(a.sale_date) = 1 then a.num end), 0) Sunday
from order_detail_2 a
         right join product_detail2 b -- 没有销售的产品也需要列出,所以以产品表为基表
                    on a.id = b.product_id
group by b.category
;

select *
from order_detail_2
where id in (1, 2)
  and `dayofweek`(sale_date) = 6;

-- 35. 查看每件商品的售价涨幅情况，按照涨幅升序排序

drop table if exists price_change;

create table price_change
(
    product_id varchar(32) comment '商品id',
    price      varchar(32) comment '商品价格',
    start_date date comment '起始日期',
    end_date   date comment '结束日期'
) comment '商品售价变化明细表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/price_change';

INSERT INTO price_change
VALUES ('1', '5000', '2020-01-01', '2020-01-01'),
       ('1', '4500', '2020-01-01', '9999-01-01'),
       ('2', '6000', '2020-02-01', '9999-01-01'),
       ('3', '3000', '2020-03-01', '2020-03-08'),
       ('3', '4000', '2020-03-08', '9999-01-01');

-- 商品售价变化明细表
select *
from price_change;

-- 这件商品什么时候涨的，涨了多少
select product_id,
       sum(next_price)  as next_price,
       max(change_date) as change_date
from (
         select product_id,
                - price + lead(price, 1, price) over (partition by product_id order by start_date asc) as next_price,
                lead(start_date, 1, start_date) over (partition by product_id order by start_date asc) as change_date
         from price_change
     ) as table_temp
group by product_id
order by next_price asc;

-- 36. 销售订单首购和次购分析：
--     如果用户成功下单两个及以上数量的手机(xiaomi, apple, vivo)订单，那么输出 -> 用户id, 首次购买成功日期, 二次购买成功日期， 成功购买次数

drop table if exists order_info_2;

create table order_info_2
(
    order_id     varchar(32) comment '商品id',
    user_id      varchar(32) comment '用户id',
    product_name varchar(32) comment '商品名称',
    status       varchar(32) comment '是否购买成功',
    `date`       date comment '购买日期'
) comment '订单详情表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/order_info_2';

INSERT INTO order_info_2
VALUES ('1', '100001', 'xiaomi', 'fail', '2021-01-01'),
       ('2', '100002', 'apple', 'success', '2021-01-02'),
       ('3', '100003', 'xiyiji', 'success', '2021-01-03'),
       ('4', '100003', 'xiaomi', 'success', '2021-01-04'),
       ('5', '100001', 'vivo', 'success', '2021-01-03'),
       ('6', '100003', 'vivo', 'success', '2021-01-08'),
       ('8', '100001', 'apple', 'success', '2021-01-06'),
       ('7', '100001', 'xiaomi', 'success', '2021-01-05');

-- 订单详情表
select *
from order_info_2;

with temp_table as (
    select *, 1 as flag
    from order_info_2
    where product_name in ('xiaomi', 'apple', 'vivo')
      and status = 'success'
)
select user_id,
       min(`date`) as first,
       max(`date`) as second,
       success_order_nums
from (
         select user_id,
                `date`,
                rank() over (partition by user_id order by `date` asc) as ranking,
                count(order_id) over (partition by user_id)            as success_order_nums
         from temp_table
     ) as table_temp
where ranking <= 2
  and success_order_nums >= 2
group by user_id, success_order_nums
order by user_id asc;

-- 37. 现在有各个商品的当天售卖明细表，需要求出同一个商品在2021年和2022年中同一个月的售卖情况对比。

drop table if exists product_detail_3;

create table product_detail_3
(
    order_id     varchar(32) comment '商品id',
    product_name varchar(32) comment '商品名称',
    num          int comment '商品售卖件数',
    `date`       date comment '购买日期'
) comment '商品明细表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/product_detail_3';

INSERT INTO product_detail_3
VALUES ('1', 'xiaomi', 53, '2021-01-02'),
       ('2', 'apple', 23, '2021-01-02'),
       ('3', 'vivo', 12, '2021-01-02'),
       ('4', 'xiaomi', 54, '2021-01-03'),
       ('5', 'apple', 43, '2021-01-03'),
       ('6', 'vivo', 41, '2021-01-03'),
       ('7', 'vivo', 24, '2021-02-03'),
       ('8', 'xiaomi', 23, '2021-02-03'),
       ('9', 'apple', 34, '2021-02-03'),
       ('10', 'vivo', 42, '2021-02-04'),
       ('11', 'xiaomi', 45, '2021-02-04'),
       ('12', 'apple', 59, '2021-02-04'),
       ('13', 'xiaomi', 230, '2022-01-04'),
       ('14', 'vivo', 764, '2022-01-04'),
       ('15', 'apple', 644, '2022-01-04'),
       ('16', 'xiaomi', 240, '2022-01-06'),
       ('17', 'vivo', 714, '2022-01-06'),
       ('18', 'apple', 624, '2022-01-06'),
       ('19', 'xiaomi', 260, '2022-01-04'),
       ('20', 'vivo', 721, '2022-02-14'),
       ('21', 'apple', 321, '2022-02-14'),
       ('22', 'xiaomi', 134, '2022-02-14'),
       ('23', 'vivo', 928, '2022-02-24'),
       ('24', 'apple', 525, '2022-02-24'),
       ('25', 'xiaomi', 231, '2020-02-06');

-- 商品售卖明细表
select *
from product_detail_3;

with temp_table as (
    select product_name,
           substr(`date`, 1, 7) as year_month,
           sum(num)             as total_nums
    from product_detail_3
    where year(`date`) in (2021, 2022)
    group by product_name, substr(`date`, 1, 7)
)
select t1.product_name,
       t1.year_month,
       t1.total_nums,
       t2.year_month,
       t2.total_nums
from temp_table t1
         join temp_table t2
              on t1.product_name = t2.product_name and substr(t1.year_month, 1, 4) != substr(t2.year_month, 1, 4) and
                 substr(t1.year_month, 6, 2) = substr(t2.year_month, 6, 2) and t1.year_month < t2.year_month
order by product_name asc, t1.year_month asc;

-- 38. 库存最多的商品

drop table if exists product_detail_10;

create table product_detail_10
(
    id     varchar(32) comment '商品id',
    `date` date comment '变化时间',
    action varchar(32) comment '补货或者售货',
    amount int comment '补货或者售货数量'
) comment '商品明细表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/product_detail';

INSERT INTO product_detail_10
VALUES ('1', '2021-01-01', 'supply', 2000),
       ('1', '2021-01-03', 'sell', 1000),
       ('1', '2021-01-05', 'supply', 3000),
       ('2', '2021-01-01', 'supply', 7000),
       ('2', '2021-01-01', 'supply', 1000),
       ('2', '2021-01-04', 'sell', 8000),
       ('3', '2021-01-01', 'supply', 4000),
       ('4', '2021-01-01', 'supply', 3000),
       ('4', '2021-01-03', 'supply', 1000),
       ('5', '2021-01-01', 'supply', 2000);

-- 商品明细表
select *
from product_detail_10;

select id,
       stock
from (
         select id,
                rank() over (partition by flag order by stock desc) as ranking,
                stock
         from (
                  select id,
                         sum(`if`(action = 'supply', amount, -amount)) as stock,
                         1                                             as flag
                  from product_detail_10
                  group by id
              ) as table_temp) as table_temp
where ranking = 1
order by id asc;

-- 39. 统计国庆前三天的每一天的最近一周的每个品类下商品收藏量和购买量，假设前三天每天的最近一周都有记录

drop table if exists product_prop;

create table product_prop
(
    id       varchar(32) comment '商品id',
    name     varchar(32) comment '商品名字',
    category varchar(32) comment '商品品类名'
) comment '商品属性表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/product_prop';

drop table if exists product_purchase;

create table product_purchase
(
    id      varchar(32) comment '商品id',
    user_id varchar(32) comment '购买用户id',
    `date`  date comment '时间'
) comment '商品购买明细表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/product_purchase';

drop table if exists product_favor;

create table product_favor
(
    id      varchar(32) comment '商品id',
    user_id varchar(32) comment '收藏用户id',
    `date`  date comment '时间'
) comment '商品收藏明细表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/product_favor';

INSERT INTO product_prop
VALUES ('1', 'xiaomi', 'phone'),
       ('2', 'vivo', 'phone'),
       ('3', 'apple', 'phone'),
       ('4', 'dami', 'food'),
       ('5', 'kuzi', 'cloth');

INSERT INTO product_purchase
VALUES ('1', '1002', '2021-09-25'),
       ('1', '1003', '2021-09-27'),
       ('1', '1006', '2021-10-01'),
       ('1', '1007', '2021-10-03'),
       ('2', '1001', '2021-09-24'),
       ('2', '1002', '2021-09-25'),
       ('2', '1003', '2021-09-27'),
       ('2', '1005', '2021-09-30'),
       ('2', '1006', '2021-10-01'),
       ('2', '1007', '2021-10-02'),
       ('3', '1001', '2021-09-24'),
       ('3', '1002', '2021-09-25'),
       ('3', '1003', '2021-09-29'),
       ('3', '1005', '2021-09-30'),
       ('3', '1006', '2021-10-01'),
       ('3', '1007', '2021-10-02'),
       ('4', '1001', '2021-09-24'),
       ('4', '1002', '2021-09-25'),
       ('4', '1003', '2021-09-26'),
       ('5', '1005', '2021-09-27'),
       ('5', '1006', '2021-10-01'),
       ('5', '1008', '2021-10-02'),
       ('5', '1007', '2021-10-03');

INSERT INTO product_favor
VALUES ('1', '1001', '2021-09-24'),
       ('1', '1002', '2021-09-25'),
       ('1', '1003', '2021-09-26'),
       ('1', '1005', '2021-09-30'),
       ('1', '1006', '2021-10-01'),
       ('1', '1007', '2021-10-03'),
       ('2', '1001', '2021-09-24'),
       ('2', '1002', '2021-09-25'),
       ('2', '1003', '2021-09-26'),
       ('2', '1005', '2021-09-30'),
       ('2', '1006', '2021-10-01'),
       ('2', '1007', '2021-10-02'),
       ('3', '1001', '2021-09-24'),
       ('3', '1002', '2021-09-25'),
       ('3', '1003', '2021-09-26'),
       ('3', '1005', '2021-09-30'),
       ('3', '1006', '2021-10-01'),
       ('3', '1007', '2021-10-02'),
       ('4', '1001', '2021-09-24'),
       ('4', '1002', '2021-09-25'),
       ('4', '1003', '2021-09-26'),
       ('5', '1005', '2021-09-27'),
       ('5', '1006', '2021-10-01'),
       ('5', '1007', '2021-10-03');

-- 商品属性表
select *
from product_prop;

-- 商品购买明细表
select *
from product_purchase;

-- 商品收藏明细表
select *
from product_favor;
;



select temp_1.category,
       temp_1.`date`,
       nvl(buy_total_nums, 0) as buy_total_nums
from (select *
      from (select category
            from product_prop
            group by category) as temp_1,
           (select date_sub('2021-10-01', 1) as `date`
            union
            select date_sub('2021-10-01', 2) as `date`
            union
            select date_sub('2021-10-01', 3) as `date`) as temp_2) as temp_1
         left join (
    select category,
           `date`,
           buy_total_nums
    from (
             select category,
                    `date`,
                    sum(buy_total_nums)
                        over (partition by category order by `date` asc rows between 6 preceding and current row ) as buy_total_nums
             from (
                      select category,
                             `date`,
                             count(*) as buy_total_nums
                      from product_purchase u
                               left join product_prop r
                                         on u.id = r.id
                      group by category, `date`
                  ) as temp
         ) as table_temp
    where `date` in (date_sub('2021-10-01', 1), date_sub('2021-10-01', 2), date_sub('2021-10-01', 3))
    group by category, `date`, buy_total_nums
) as temp_2
                   on temp_1.category = temp_2.category and temp_1.`date` = temp_2.`date`;


select *
from product_purchase
         left join product_prop
order by category asc, `date` asc;

-- 40. 每个商品同一时刻最多浏览人数
--     统计每个商品同一时刻最多的在浏览人数，如果同一时刻有进入也有离开，先记录用户数增加再记录减少，按照最大的人数降序排序

drop table if exists user_action_log;

create table user_action_log
(
    user_id    varchar(32) comment '用户id',
    id         varchar(32) comment '商品id',
    start_time timestamp not null comment '起始时间',
    end_time   timestamp not null comment '起始时间'
) comment '用户行为日志表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/user_action_log';

INSERT INTO user_action_log
VALUES ('101', '9001', '2021-11-01 02:00:00', '2021-11-01 02:00:11'),
       ('102', '9001', '2021-11-01 02:00:09', '2021-11-01 02:00:38'),
       ('103', '9001', '2021-11-01 02:00:28', '2021-11-01 02:00:58'),
       ('104', '9002', '2021-11-01 03:00:45', '2021-11-01 03:01:11'),
       ('105', '9001', '2021-11-01 02:00:51', '2021-11-01 02:00:59'),
       ('106', '9002', '2021-11-01 03:00:55', '2021-11-01 03:01:24'),
       ('107', '9001', '2021-11-01 02:00:01', '2021-11-01 02:01:50');

-- 用户行为日志表
select *
from user_action_log;

with temp_table as (
    select user_id,
           id,
           start_time as `time`,
           1          as flag
    from user_action_log
    union all
    select user_id,
           id,
           end_time as `time`,
           -1       as flag
    from user_action_log
)
select id,
       max(total_num) as max_nums
from (
         select id,
                `time`,
                sum(flag)
                    over (partition by id order by `time` asc rows between unbounded preceding and current row) as total_num
         from temp_table
     ) as table_temp
group by id
order by id asc;

-- 41. 统计活跃间隔对用户分级结果：
--     用户等级：-> 忠实用户：近七天活跃且非新用户
--             -> 新晋用户：近七天新增
--             -> 沉睡用户：近七天未活跃但在7天前活跃
--             -> 流失用户：近30天未活跃，但在30天前活跃
--             -> 假设今天是数据中所有日期的最大值

drop table if exists user_action_log_2;

create table user_action_log_2
(
    user_id    varchar(32) comment '用户id',
    id         varchar(32) comment '商品id',
    start_time timestamp not null comment '起始时间',
    end_time   timestamp not null comment '起始时间'
) comment '用户行为日志表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/user_action_log_2';

INSERT INTO user_action_log_2
VALUES ('109', ' 9001', '2021-08-31 02:00:00', '2021-08-31 02:00:09'),
       ('109', ' 9002', '2021-11-04 03:00:55', '2021-11-04 03:00:59'),
       ('108', ' 9001', '2021-09-01 02:00:01', '2021-09-01 02:01:50'),
       ('108', ' 9001', '2021-11-03 02:00:01', '2021-11-03 02:01:50'),
       ('104', ' 9001', '2021-11-02 02:00:28', '2021-11-02 02:00:50'),
       ('104', ' 9003', '2021-09-03 03:00:45', '2021-09-03 03:00:55'),
       ('105', ' 9003', '2021-11-03 03:00:53', '2021-11-03 03:00:59'),
       ('102', ' 9001', '2021-10-30 02:00:00', '2021-10-30 02:00:09'),
       ('103', ' 9001', '2021-10-21 02:00:00', '2021-10-21 02:00:09'),
       ('101', ' 9001', '2021-10-01 02:00:00', '2021-10-01 02:00:42');

-- 用户行为日志表
select *
from user_action_log_2;


with temp_table as (
    select distinct user_id,
                    case
                        when recent_active >= date_sub(today, 7) and first_register < date_sub(today, 7) then '忠实用户'
                        when first_register >= date_sub(today, 7) then '新晋用户'
                        when recent_active < date_sub(today, 7) and recent_active >= date_sub(today, 30) then '沉睡用户'
                        else '流失用户'
                        end as user_level
    from (
             select user_id,
                    max(active_date) over (partition by user_id) as recent_active,
                    max(active_date) over (partition by flag)    as today,
                    min(active_date) over (partition by user_id) as first_register
             from (
                      select user_id,
                             date_format(start_time, 'yyyy-MM-dd') as active_date,
                             1                                     as flag
                      from user_action_log_2
                  ) as table_temp
         ) as table_temp
)
select user_level,
       round(count(user_id) / (select count(user_id) from temp_table), 2) as rate
from temp_table
group by user_level
order by rate desc;

-- 42. 连续签到领金币数
--     用户每天签到可领1金币，可以累积签到天数，连续签到第3、7天分别可以额外领2和6金币，每连续7天重新累积签到天数
--     计算从2021年7月以来每个月获得的金币数,结果按照月份、id升序排序

drop table if exists user_sign;

create table user_sign
(
    user_id    varchar(32) comment '用户id',
    id         varchar(32) comment '商品id',
    start_time timestamp not null comment '起始时间',
    end_time   timestamp not null comment '起始时间',
    sign       int comment '是否签到，1为签到，0为未签到'
) comment '用户签到表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/user_sign';

INSERT INTO user_sign
VALUES ('101', '0', '2021-07-07 02:00:00', '2021-07-07 02:00:09', 1),
       ('101', '0', '2021-07-08 02:00:00', '2021-07-08 02:00:09', 1),
       ('101', '0', '2021-07-09 02:00:00', '2021-07-09 02:00:42', 1),
       ('101', '0', '2021-07-10 02:00:00', '2021-07-10 02:00:09', 1),
       ('101', '0', '2021-07-11 15:59:55', '2021-07-11 15:59:59', 1),
       ('101', '0', '2021-07-12 02:00:28', '2021-07-12 02:00:50', 1),
       ('101', '0', '2021-07-13 02:00:28', '2021-07-13 02:00:50', 1),
       ('102', '0', '2021-10-01 02:00:28', '2021-10-01 02:00:50', 1),
       ('102', '0', '2021-10-02 02:00:01', '2021-10-02 02:01:50', 1),
       ('102', '0', '2021-10-03 03:00:55', '2021-10-03 03:00:59', 1),
       ('102', '0', '2021-10-04 03:00:45', '2021-10-04 03:00:55', 0),
       ('102', '0', '2021-10-05 03:00:53', '2021-10-05 03:00:59', 1),
       ('102', '0', '2021-10-06 03:00:45', '2021-10-06 03:00:55', 1);

-- 用户签到表
select *
from user_sign;

with temp_table as (
    select user_id,
           date_format(start_time, 'yyyy-MM-dd') as sign_day
    from user_sign
    where date_format(start_time, 'yyyy-MM-dd') > '2021-07'
      and sign = 1
)
select user_id,
       date_format(sign_day, 'yyyy-MM') as year_month,
       sum(gold_coin)                   as gold_coins
from (
         select user_id,
                sign_day,
                case
                    when sign_flag % 7 = 3 then 3
                    when sign_flag % 7 = 0 then 7
                    else 1
                    end as gold_coin
         from (
                  select user_id,
                         sign_day,
                         rank() over (partition by sub_flag order by sign_day asc) as sign_flag
                  from (
                           select user_id,
                                  sign_day,
                                  date_sub(sign_day,
                                           rank() over (partition by user_id order by sign_day asc)) as sub_flag
                           from temp_table
                       ) as temp_1
              ) as temp_2
     ) as temp_3
group by user_id, date_format(sign_day, 'yyyy-MM')
order by year_month asc, user_id asc;

set hive.execution.engine=mr;

-- 43. 统计2021年10月每个有展示记录的退货率不大于0.5的商品各项指标
--     商品点展比 = 点击数 ÷ 展示数
--     加购率 = 加购数 ÷ 点击数
--     成单率 = 付款数 ÷ 加购数
--     退货率 = 退款数 ÷ 付款数
--     当分母为0时，整体结果为0，结果中各项指标保留3位小数，按照商品id升序排序

drop table if exists user_action_2;

create table user_action_2
(
    user_id varchar(32) comment '用户id',
    id      varchar(32) comment '商品id',
    `time`  timestamp not null comment '事件时间',
    click   int comment '点击，1为是',
    cart    int comment '购物车，1为是',
    payment int comment '付款，1为是',
    refund  int comment '退货，1为是'
) comment '用户行为统计表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/user_action_2';

set hive.execution.engine=mr;

set hive.execution.engine=spark;

INSERT INTO user_action_2
VALUES ('101', ' 8001', '2021-10-01 02:00:00', 0, 0, 0, 0),
       ('102', ' 8001', '2021-10-01 02:00:00', 1, 0, 0, 0),
       ('103', ' 8001', '2021-10-01 02:00:00', 1, 1, 0, 0),
       ('104', ' 8001', '2021-10-02 02:00:00', 1, 1, 1, 0),
       ('105', ' 8001', '2021-10-02 02:00:00', 1, 1, 1, 0),
       ('101', ' 8002', '2021-10-03 02:00:00', 1, 1, 1, 0),
       ('109', ' 8001', '2021-10-04 02:00:00', 1, 1, 1, 1);

-- 用户行为统计表
select *
from user_action_2;

select id,
       `商品点展比`,
       `加购率`,
       `成单率`,
       `退货率`
from (
         select id,
                `if`(view_total = 0, 0, round(click_total / view_total, 3))        as `商品点展比`,
                `if`(click_total = 0, 0, round(cart_total / click_total, 3))       as `加购率`,
                `if`(cart_total = 0, 0, round(payment_total / cart_total, 3))      as `成单率`,
                `if`(payment_total = 0, 0, round(refund_total / payment_total, 3)) as `退货率`
         from (
                  select id,
                         (select count(*) from user_action_2 t2 where t2.id = t1.id) as view_total,
                         sum(click)                                                  as click_total,
                         sum(cart)                                                   as cart_total,
                         sum(payment)                                                as payment_total,
                         sum(refund)                                                 as refund_total
                  from user_action_2 t1
                  group by id
              ) as temp_1
     ) as temp_2
where `退货率` <= 0.5
order by id asc;

-- 44. 计算2021年10月商城里面所有新用户的首单平均交易金额（客单价）和平均获客成本（保留一位小数）。
--     订单的优惠金额 = 订单明细里的{该订单各商品单价 × 数量之和} - 订单总表里的{订单总金额}
--     平均获客成本 = 平均优惠金额

drop table if exists order_detail_10;

create table order_detail_10
(
    order_id varchar(32) comment '订单id',
    id       varchar(32) comment '商品id',
    price    int comment '单价价格',
    count    int comment '商品个数'
) comment '订单明细表'
    row format delimited fields terminated by ''
        null defined as ''
    location '/warerhouse/sdc/rds/order_detail_10';

INSERT INTO order_detail_10
VALUES ('301002', '8001', 85, 1),
       ('301002', '8003', 180, 1),
       ('301003', '8004', 140, 1),
       ('301003', '8003', 180, 1),
       ('301005', '8003', 180, 1),
       ('301006', '8003', 180, 1);

-- 订单明细表
select *
from order_detail_10;

-- 订单表
select *
from order_info3;

with temp_table as (
    select order_id,
           sum(price) as actual_total_amount
    from order_detail_10
    group by order_id
)
select user_id,
       avg(total_amount) over (partition by flag)                       as avg_amount,
       avg(actual_total_amount - total_amount) over (partition by flag) as avg_base
from (select *, 1 as flag
      from order_info3
      where date_format(event_time, 'yyyy-MM') = '2021-10') as temp_1
         left join temp_table
                   on temp_1.order_id = temp_table.order_id;

-- 45. 国庆期间的7日动销率和滞销率
--      动销率：店铺中一段时间内有销量的商品占当前已上架总商品数的比例（有销量的商品/已上架总商品数）
--      滞销率：店铺中一段时间内没有销量的商品占当前已上架总商品数的比例（没有销量的商品/已上架的总商品数量）
--      只要当天任一店铺有任何商品的销量就输出该天的结果，及时店铺当天的动销率为0

drop table if exists order_detail_11;

create table order_detail_11
(
    order_id varchar(32) comment '订单id',
    id       varchar(32) comment '商品id',
    price    int comment '单价价格',
    count    int comment '商品个数'
) comment '订单明细表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/order_detail_11';

drop table if exists order_info_3;

create table order_info_3
(
    order_id     varchar(32) comment '订单id',
    user_id      varchar(32) comment '用户id',
    event_time   date comment '时间',
    total_amount int comment '订单总额',
    total_count  int comment '订单中商品个数'
) comment '订单表'
    row format delimited fields terminated by '\t'
        null defined as ''
    location '/warehouse/sdc/rds/order_info_3';

INSERT INTO order_detail_11
VALUES ('301004', '8002', 180, 1),
       ('301005', '8002', 170, 1),
       ('301002', '8001', 85, 1),
       ('301002', '8003', 180, 1),
       ('301003', '8002', 150, 1),
       ('301003', '8003', 180, 1);

INSERT INTO order_info_3
VALUES ('301004', '102', '2021-09-30', 170, 1),
       ('301005', '104', '2021-10-01', 160, 1),
       ('301003', '101', '2021-10-02', 300, 2),
       ('301002', '102', '2021-10-03', 235, 2);

-- 订单明细表
select *
from order_detail_11;

-- 明细表
select *
from order_info_3;

with temp_table as (
    select order_id,
           count(distinct id) as selled_total,
           online_total
    from (
             select order_id,
                    id,
                    count(distinct id) over (partition by flag) as online_total
             from (
                      select *, 1 as flag
                      from order_detail_11
                  ) as temp_1
         ) as temp_3
    group by order_id, online_total
)
select event_time,
       round(selled_total / online_total, 3)     as `动销率`,
       round(1 - selled_total / online_total, 3) as `滞销率`
from order_info_3 o
         left join temp_table t
                   on o.order_id = t.order_id
where date_format(event_time, 'yyyy-MM-dd') between '2021-10-01' and '2021-10-07'
order by event_time asc;