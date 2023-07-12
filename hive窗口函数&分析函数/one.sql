-- hive开窗

-- sum,avg,min,max 函数

-- 建表语句
create table big_data_t1
(
    cookie_id   string,
    create_time string,
    pv          int
) comment ''
    row format delimited fields terminated by ','
    location '/warehouse/test/big_data_t1';

load data local inpath '/opt/module/datas/big_data_t1.dat' into table big_data_t1;

select *
from big_data_t1;

-- 原数据
-- cookie1,    2018-04-10,    1
-- cookie1,    2018-04-11,    5
-- cookie1,    2018-04-12,    7
-- cookie1,    2018-04-13,    3
-- cookie1,    2018-04-14,    2
-- cookie1,    2018-04-15,    4
-- cookie1,    2018-04-16,    4


-- 开启智能本地模式
set hive.exec.mode.local.auto=true;

-- pv1
-- 分组内从起点到当前行的累积
select cookie_id,
       create_time,
       pv,
       sum(pv) over (partition by cookie_id order by create_time) as pv1
from big_data_t1;

-- 查询结果
-- cookie1,    2018-04-10,    1,    1
-- cookie1,    2018-04-11,    5,    6
-- cookie1,    2018-04-12,    7,    13
-- cookie1,    2018-04-13,    3,    16
-- cookie1,    2018-04-14,    2,    18
-- cookie1,    2018-04-15,    4,    22
-- cookie1,    2018-04-16,    4,    26

-- pv2
-- 分组内从起点到当前行的累积
select cookie_id,
       create_time,
       pv,
       sum(pv)
           over (partition by cookie_id order by create_time rows between unbounded preceding and current row ) as pv2
from big_data_t1;

-- cookie1,    2018-04-10,    1,    1
-- cookie1,    2018-04-11,    5,    6
-- cookie1,    2018-04-12,    7,    13
-- cookie1,    2018-04-13,    3,    16
-- cookie1,    2018-04-14,    2,    18
-- cookie1,    2018-04-15,    4,    22
-- cookie1,    2018-04-16,    4,    26

-- pv3
-- 分组内聚合所有行的pv值
select cookie_id,
       create_time,
       pv,
       sum(pv) over (partition by cookie_id) as pv3
from big_data_t1;

-- cookie1,    2018-04-10,    1,    26
-- cookie1,    2018-04-11,    5,    26
-- cookie1,    2018-04-12,    7,    26
-- cookie1,    2018-04-13,    3,    26
-- cookie1,    2018-04-14,    2,    26
-- cookie1,    2018-04-15,    4,    26
-- cookie1,    2018-04-16,    4,    26

-- pv4
-- 分组内聚合当前行与其前三行的pv值
-- 分组内当前行+往前3行. 如: 11号=10号+11号, 12号=10号+11号+12号, 13号=10号+11号+12号+13号, 14号=11号+12号+13号+14号
select cookie_id,
       create_time,
       pv,
       sum(pv) over (partition by cookie_id order by create_time rows between 3 preceding and current row ) as pv4
from big_data_t1;

-- cookie1,    2018-04-10,    1,    1
-- cookie1,    2018-04-11,    5,    6
-- cookie1,    2018-04-12,    7,    13
-- cookie1,    2018-04-13,    3,    16
-- cookie1,    2018-04-14,    2,    17
-- cookie1,    2018-04-15,    4,    16
-- cookie1,    2018-04-16,    4,    13

-- pv5
-- 分组内当前行+往前3行+往后1行，如，14号=11号+12号+13号+14号+15号=5+7+3+2+4=21
select cookie_id,
       create_time,
       pv,
       sum(pv) over (partition by cookie_id order by create_time rows between 3 preceding and 1 following) as pv5
from big_data_t1;

-- cookie1,    2018-04-10,    1,    6
-- cookie1,    2018-04-11,    5,    13
-- cookie1,    2018-04-12,    7,    16
-- cookie1,    2018-04-13,    3,    18
-- cookie1,    2018-04-14,    2,    21
-- cookie1,    2018-04-15,    4,    20
-- cookie1,    2018-04-16,    4,    13

-- pv6
-- 分组内当前行+往后所有行. 如: 13号=13号+14号+15号+16号=3+2+4+4=13, 14号=14号+15号+16号=2+4+4=10
select cookie_id,
       create_time,
       pv,
       sum(pv)
           over (partition by cookie_id order by create_time rows between current row and unbounded following) as pv6
from big_data_t1;

-- cookie1,    2018-04-10,    1,    26
-- cookie1,    2018-04-11,    5,    25
-- cookie1,    2018-04-12,    7,    20
-- cookie1,    2018-04-13,    3,    13
-- cookie1,    2018-04-14,    2,    10
-- cookie1,    2018-04-15,    4,    8
-- cookie1,    2018-04-16,    4,    4

-- 总结:
-- 不指定rows between, 默认从起点到当前行;
-- 不指定order by, 则将分组内所有值累加;

-- rows between,也叫做window子句：
-- preceding：往前
-- following：往后
-- current row：当前行
-- unbounded：起点
-- unbounded preceding 表示从前面的起点
-- unbounded following：表示到后面的终点

-- AVG，MIN，MAX，和SUM用法一样。

-- row_number,rank,dense_rank,ntile函数
-- 建表语句
create table big_data_t2
(
    cookie_id   string,
    create_time string, --day
    pv          int
) comment ''
    row format delimited fields terminated by ','
    stored as textfile
    location '/warehouse/test/big_data_t2';

load data local inpath '/opt/module/datas/big_data_t2.dat' into table big_data_t2;

select *
from big_data_t2;

-- 原数据
-- cookie1,    2018-04-10,    1
-- cookie1,    2018-04-11,    5
-- cookie1,    2018-04-12,    7
-- cookie1,    2018-04-13,    3
-- cookie1,    2018-04-14,    2
-- cookie1,    2018-04-15,    4
-- cookie1,    2018-04-16,    4
-- cookie2,    2018-04-10,    2
-- cookie2,    2018-04-11,    3
-- cookie2,    2018-04-12,    5
-- cookie2,    2018-04-13,    6
-- cookie2,    2018-04-14,    3
-- cookie2,    2018-04-15,    9
-- cookie2,    2018-04-16,    7

-- row_number
-- 从1开始，按照顺序，生成分组内记录的序列
select *,
       row_number() over (partition by cookie_id order by pv desc ) as rn
from big_data_t2;

-- cookie1,    2018-04-12,    7,    1
-- cookie1,    2018-04-11,    5,    2
-- cookie1,    2018-04-15,    4,    3
-- cookie1,    2018-04-16,    4,    4
-- cookie1,    2018-04-13,    3,    5
-- cookie1,    2018-04-14,    2,    6
-- cookie1,    2018-04-10,    1,    7
-- cookie2,    2018-04-15,    9,    1
-- cookie2,    2018-04-16,    7,    2
-- cookie2,    2018-04-13,    6,    3
-- cookie2,    2018-04-12,    5,    4
-- cookie2,    2018-04-11,    3,    5
-- cookie2,    2018-04-14,    3,    6
-- cookie2,    2018-04-10,    2,    7

-- RANK 和 DENSE_RANK使用
-- RANK() 组内排名，排名相等会在名次中留下空位。
--
-- DENSE_RANK() 组内排名，排名相等在名次中不会留下空位。
select *,
       rank() over (partition by cookie_id order by pv desc )       as `rank`,
       dense_rank() over (partition by cookie_id order by pv desc ) as `dense_rank`,
       row_number() over (partition by cookie_id order by pv desc ) as `row_number`
from big_data_t2
where cookie_id = 'cookie1';

-- cookie1,    2018-04-12,    7,    1,    1,    1
-- cookie1,    2018-04-11,    5,    2,    2,    2
-- cookie1,    2018-04-15,    4,    3,    3,    3
-- cookie1,    2018-04-16,    4,    3,    3,    4
-- cookie1,    2018-04-13,    3,    5,    4,    5
-- cookie1,    2018-04-14,    2,    6,    5,    6
-- cookie1,    2018-04-10,    1,    7,    6,    7

-- ntile
-- ntile可以看成是：把有序的数据集合平均分配到指定的数量（num）个桶中, 将桶号分配给每一行。如果不能平均分配，则优先分配较小编号的桶，并且各个桶中能放的行数最多相差1。

select *,
       ntile(2) over (partition by cookie_id order by create_time) as ntile_2,
       ntile(3) over (partition by cookie_id order by create_time) as ntile_3,
       ntile(4) over (order by create_time)                        as ntile_4
from big_data_t2
order by cookie_id, create_time;

-- cookie1,    2018-04-10,    1,    1,    1,    1
-- cookie2,    2018-04-10,    2,    1,    1,    1
-- cookie1,    2018-04-11,    5,    1,    1,    1
-- cookie2,    2018-04-11,    3,    1,    1,    1
-- cookie1,    2018-04-12,    7,    1,    1,    2
-- cookie2,    2018-04-12,    5,    1,    1,    2
-- cookie1,    2018-04-13,    3,    1,    2,    2
-- cookie2,    2018-04-13,    6,    1,    2,    2
-- cookie1,    2018-04-14,    2,    2,    2,    3
-- cookie2,    2018-04-14,    3,    2,    2,    3
-- cookie1,    2018-04-15,    4,    2,    3,    3
-- cookie2,    2018-04-15,    9,    2,    3,    4
-- cookie1,    2018-04-16,    4,    2,    3,    4
-- cookie2,    2018-04-16,    7,    2,    3,    4

-- lag,lead,first_value,last_value 函数
SELECT cookie_id,
       create_time,
       pv,
       ROW_NUMBER() OVER (PARTITION BY cookie_id ORDER BY create_time)                               AS rn,
       LAG(create_time, 1, '1970-01-01 00:00:00') OVER (PARTITION BY cookie_id ORDER BY create_time) AS last_1_time,
       LAG(create_time, 2) OVER (PARTITION BY cookie_id ORDER BY create_time)                        AS last_2_time
FROM big_data_t2;
--  last_1_time: 指定了往上第1行的值，default为'1970-01-01 00:00:00'
--                            cookie1第一行，往上1行为NULL,因此取默认值 1970-01-01 00:00:00
--                            cookie1第三行，往上1行值为第二行值，2018-04-11
--                            cookie1第六行，往上1行值为第五行值，2018-04-14
--  last_2_time: 指定了往上第2行的值，为指定默认值
--                           cookie1第一行，往上2行为NULL
--                           cookie1第二行，往上2行为NULL
--                           cookie1第四行，往上2行为第二行值，2018-04-11
--                           cookie1第七行，往上2行为第五行值，2018-04-14

-- cookie1,    2018-04-10,    1,    1,    1970-01-01 00:00:00,    <null>
-- cookie1,    2018-04-11,    5,    2,    2018-04-10,             <null>
-- cookie1,    2018-04-12,    7,    3,    2018-04-11,             2018-04-10
-- cookie1,    2018-04-13,    3,    4,    2018-04-12,             2018-04-11
-- cookie1,    2018-04-14,    2,    5,    2018-04-13,             2018-04-12
-- cookie1,    2018-04-15,    4,    6,    2018-04-14,             2018-04-13
-- cookie1,    2018-04-16,    4,    7,    2018-04-15,             2018-04-14
-- cookie2,    2018-04-10,    2,    1,    1970-01-01 00:00:00,    <null>
-- cookie2,    2018-04-11,    3,    2,    2018-04-10,             <null>
-- cookie2,    2018-04-12,    5,    3,    2018-04-11,             2018-04-10
-- cookie2,    2018-04-13,    6,    4,    2018-04-12,             2018-04-11
-- cookie2,    2018-04-14,    3,    5,    2018-04-13,             2018-04-12
-- cookie2,    2018-04-15,    9,    6,    2018-04-14,             2018-04-13
-- cookie2,    2018-04-16,    7,    7,    2018-04-15,             2018-04-14

select *,
       row_number() over (partition by cookie_id order by create_time)                       as rn,
       lead(create_time, 1, '1970-01-01') over (partition by cookie_id order by create_time) as next_1,
       lead(create_time, 2) over (partition by cookie_id order by create_time)               as next_2
from big_data_t2;

-- cookie1,    2018-04-10,    1,    1,    2018-04-11,    2018-04-12
-- cookie1,    2018-04-11,    5,    2,    2018-04-12,    2018-04-13
-- cookie1,    2018-04-12,    7,    3,    2018-04-13,    2018-04-14
-- cookie1,    2018-04-13,    3,    4,    2018-04-14,    2018-04-15
-- cookie1,    2018-04-14,    2,    5,    2018-04-15,    2018-04-16
-- cookie1,    2018-04-15,    4,    6,    2018-04-16,    <null>
-- cookie1,    2018-04-16,    4,    7,    1970-01-01,    <null>
-- cookie2,    2018-04-10,    2,    1,    2018-04-11,    2018-04-12
-- cookie2,    2018-04-11,    3,    2,    2018-04-12,    2018-04-13
-- cookie2,    2018-04-12,    5,    3,    2018-04-13,    2018-04-14
-- cookie2,    2018-04-13,    6,    4,    2018-04-14,    2018-04-15
-- cookie2,    2018-04-14,    3,    5,    2018-04-15,    2018-04-16
-- cookie2,    2018-04-15,    9,    6,    2018-04-16,    <null>
-- cookie2,    2018-04-16,    7,    7,    1970-01-01,    <null>

-- first_value: 分组排序后截止到当前行的第一个值
-- last_value: 分组排序后截止到当前行的最后一个值
-- 不指定order by会排序混乱，结果错误

select *,
       row_number() over (partition by cookie_id order by create_time)         as rn,
       first_value(pv) over (partition by cookie_id order by create_time desc) as `first_value`,
       last_value(pv) over (partition by cookie_id order by create_time)       as `last_value`
from big_data_t2;

-- cookie1,    2018-04-16,    4,    7,    4,    4
-- cookie1,    2018-04-15,    4,    6,    4,    4
-- cookie1,    2018-04-14,    2,    5,    4,    2
-- cookie1,    2018-04-13,    3,    4,    4,    3
-- cookie1,    2018-04-12,    7,    3,    4,    7
-- cookie1,    2018-04-11,    5,    2,    4,    5
-- cookie1,    2018-04-10,    1,    1,    4,    1
-- cookie2,    2018-04-16,    7,    7,    7,    7
-- cookie2,    2018-04-15,    9,    6,    7,    9
-- cookie2,    2018-04-14,    3,    5,    7,    3
-- cookie2,    2018-04-13,    6,    4,    7,    6
-- cookie2,    2018-04-12,    5,    3,    7,    5
-- cookie2,    2018-04-11,    3,    2,    7,    3
-- cookie2,    2018-04-10,    2,    1,    7,    2

-- cume_dist,percent_rank 函数
-- 不常用, 序列函数不支持window子句
drop table big_data_t3;

create table big_data_t3
(
    dept_id string,
    user_id string,
    salary  int
) comment ''
    row format delimited fields terminated by ','
    stored as textfile
    location '/warehouse/test/big_data_t3';

load data local inpath '/opt/module/datas/big_data_t3.dat' into table big_data_t3;

insert into table big_data_t3
values (' d1', 'user4', 4000);

select *
from big_data_t3;

--  d1,user4,4000
--  d1,user1,1000
--  d1,user2,2000
--  d1,user3,3000
--  d2,user4,4000
--  d2,user5,5000


-- cume_dist
-- 小于等于当前值的行数/分组内总行数  order 默认顺序 正序 升序
-- 比如，统计小于等于当前薪水的人数，所占总人数的比例
select *,
       cume_dist() over (order by salary)                      as `cume_dist_1`,
       cume_dist() over (partition by dept_id order by salary) as `cume_dist_2`
from big_data_t3;

--  d1,        user1,        1000,        0.2,        0.3333333333333333
--  d1,        user2,        2000,        0.4,        0.6666666666666666
--  d1,        user3,        3000,        0.6,        1
--  d2,        user4,        4000,        0.8,        0.5
--  d2,        user5,        5000,        1,          1

-- PERCENT_RANK
-- 分组内当前行的RANK值-1/分组内总行数-1

SELECT dept_id,
       user_id,
       salary,
       PERCENT_RANK() OVER (ORDER BY salary)                      AS `percent_rank`, --分组内
       RANK() OVER (ORDER BY salary)                              AS `rank`,         --分组内RANK值
       SUM(1) OVER (PARTITION BY NULL)                            AS `sum`,          --分组内总行数
       PERCENT_RANK() OVER (PARTITION BY dept_id ORDER BY salary) AS `percent_rank`
FROM big_data_t3;

--  d1,        user1,        1000,        0,          1,        6,        0
--  d1,        user2,        2000,        0.2,        2,        6,        0.3333333333333333
--  d1,        user3,        3000,        0.4,        3,        6,        0.6666666666666666
--  d1,        user4,        4000,        0.6,        4,        6,        1
--  d2,        user4,        4000,        0.6,        4,        6,        0
--  d2,        user5,        5000,        1,          6,        6,        1

-- grouping sets,grouping__id,cube,rollup 函数
create table big_data_t4
(
    month     string,
    day       string,
    cookie_id string
) comment ''
    row format delimited fields terminated by ','
    stored as textfile
    location '/warehouse/test/big_data_t4';

load data local inpath '/opt/module/datas/big_data_t4.dat' into table big_data_t4;

select *
from big_data_t4;

-- 2018-03,    2018-03-10,    cookie1
-- 2018-03,    2018-03-10,    cookie5
-- 2018-03,    2018-03-12,    cookie7
-- 2018-04,    2018-04-12,    cookie3
-- 2018-04,    2018-04-13,    cookie2
-- 2018-04,    2018-04-13,    cookie4
-- 2018-04,    2018-04-16,    cookie4
-- 2018-03,    2018-03-10,    cookie2
-- 2018-03,    2018-03-10,    cookie3
-- 2018-04,    2018-04-12,    cookie5
-- 2018-04,    2018-04-13,    cookie6
-- 2018-04,    2018-04-15,    cookie3
-- 2018-04,    2018-04-15,    cookie2
-- 2018-04,    2018-04-16,    cookie1

-- grouping sets: 将多个group by 逻辑写在一个sql语句中, 等价于将不同维度的GROUP BY结果集进行UNION ALL。
--
-- grouping__id: 表示结果属于哪一个分组集合

SELECT month,
       day,
       COUNT(DISTINCT cookie_id) AS uv,
       GROUPING__ID
FROM big_data_t4
GROUP BY month, day
    GROUPING SETS ( month, day)
ORDER BY GROUPING__ID;

-- 2018-04,   <null>,        6,    1
-- 2018-03,   <null>,        5,    1
-- <null>,    2018-04-13,    3,    2
-- <null>,    2018-04-15,    2,    2
-- <null>,    2018-03-10,    4,    2
-- <null>,    2018-03-12,    1,    2
-- <null>,    2018-04-12,    2,    2
-- <null>,    2018-04-16,    2,    2

-- 根据grouping sets中的分组条件month，day，1是代表month，2是代表day

-- 等价于
SELECT month
     , NULL
     , COUNT(DISTINCT cookie_id) AS uv
     , 1                         AS GROUPING__ID
FROM big_data_t4
GROUP BY month
UNION ALL
SELECT NULL                      AS month
     , day
     , COUNT(DISTINCT cookie_id) AS uv
     , 2                         AS GROUPING__ID
FROM big_data_t4
GROUP BY day;

-- example2
SELECT month
     , day
     , COUNT(DISTINCT cookie_id) AS uv
     , grouping__id
FROM big_data_t4
GROUP BY month
       , day
    GROUPING SETS ( month, day, ( month, day))
ORDER BY grouping__id;

-- 2018-03,   2018-03-10,    4,    0
-- 2018-03,   2018-03-12,    1,    0
-- 2018-04,   2018-04-13,    3,    0
-- 2018-04,   2018-04-15,    2,    0
-- 2018-04,   2018-04-12,    2,    0
-- 2018-04,   2018-04-16,    2,    0
-- 2018-04,   <null>,        6,    1
-- 2018-03,   <null>,        5,    1
-- <null>,    2018-04-13,    3,    2
-- <null>,    2018-04-15,    2,    2
-- <null>,    2018-03-10,    4,    2
-- <null>,    2018-03-12,    1,    2
-- <null>,    2018-04-12,    2,    2
-- <null>,    2018-04-16,    2,    2


-- 等价于

SELECT month
     , NULL
     , COUNT(DISTINCT cookieid) AS uv
     , 1                        AS GROUPING__ID
FROM bigdata_t5
GROUP BY month
UNION ALL
SELECT NULL
     , day
     , COUNT(DISTINCT cookieid) AS uv
     , 2                        AS GROUPING__ID
FROM bigdata_t5
GROUP BY day
UNION ALL
SELECT month
     , day
     , COUNT(DISTINCT cookieid) AS uv
     , 3                        AS GROUPING__ID
FROM bigdata_t5
GROUP BY month
       , day;

-- cube: 根据GROUP BY的维度的所有组合进行聚合
select month,
       day,
       count(distinct cookie_id) as uv,
       grouping__id
from big_data_t4
group by month, day
with cube
order by grouping__id;

-- 2018-03,   2018-03-10,    4,    0
-- 2018-03,   2018-03-12,    1,    0
-- 2018-04,   2018-04-13,    3,    0
-- 2018-04,   2018-04-15,    2,    0
-- 2018-04,   2018-04-12,    2,    0
-- 2018-04,   2018-04-16,    2,    0
-- 2018-04,   <null>,        6,    1
-- 2018-03,   <null>,        5,    1
-- <null>,    2018-04-13,    3,    2
-- <null>,    2018-04-15,    2,    2
-- <null>,    2018-03-10,    4,    2
-- <null>,    2018-03-12,    1,    2
-- <null>,    2018-04-12,    2,    2
-- <null>,    2018-04-16,    2,    2
-- <null>,    <null>,        7,    3

-- 等价于
SELECT NULL
     , NULL
     , COUNT(DISTINCT cookie_id) AS uv
     , 0                         AS GROUPING__ID
FROM big_data_t4
UNION ALL
SELECT month
     , NULL
     , COUNT(DISTINCT cookie_id) AS uv
     , 1                         AS GROUPING__ID
FROM big_data_t4
GROUP BY month
UNION ALL
SELECT NULL
     , day
     , COUNT(DISTINCT cookie_id) AS uv
     , 2                         AS GROUPING__ID
FROM big_data_t4
GROUP BY day
UNION ALL
SELECT month
     , day
     , COUNT(DISTINCT cookie_id) AS uv
     , 3                         AS GROUPING__ID
FROM big_data_t4
GROUP BY month
       , day;

-- rollup: cube的子集，以最左侧的维度为主，从该维度进行层级聚合。
select month,
       day,
       count(distinct cookie_id) as uv,
       grouping__id
from big_data_t4
group by month, day
with rollup order by grouping__id;

-- 2018-03,   2018-03-10,    4,    0
-- 2018-03,   2018-03-12,    1,    0
-- 2018-04,   2018-04-13,    3,    0
-- 2018-04,   2018-04-15,    2,    0
-- 2018-04,   2018-04-12,    2,    0
-- 2018-04,   2018-04-16,    2,    0
-- 2018-04,   <null>,        6,    1
-- 2018-03,   <null>,        5,    1
-- <null>,    <null>,        7,    3

-- month与day换下顺序, 以day维度进行层级聚合, 这里，根据天和月进行聚合，和根据天聚合结果一样，因为有父子关系，如果是其他维度组合的话，就会不一样
select day,
       month,
       count(distinct cookie_id) as uv,
       grouping__id
from big_data_t4
group by day, month
with rollup order by grouping__id;

-- 2018-03-10,    2018-03,    4,    0
-- 2018-03-12,    2018-03,    1,    0
-- 2018-04-13,    2018-04,    3,    0
-- 2018-04-15,    2018-04,    2,    0
-- 2018-04-12,    2018-04,    2,    0
-- 2018-04-16,    2018-04,    2,    0
-- 2018-03-10,    <null>,     4,    1
-- 2018-03-12,    <null>,     1,    1
-- 2018-04-12,    <null>,     2,    1
-- 2018-04-16,    <null>,     2,    1
-- 2018-04-13,    <null>,     3,    1
-- 2018-04-15,    <null>,     2,    1
-- <null>,        <null>,     7,    3