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
-- cookie1,2018-04-10,1
-- cookie1,2018-04-11,5
-- cookie1,2018-04-12,7
-- cookie1,2018-04-13,3
-- cookie1,2018-04-14,2
-- cookie1,2018-04-15,4
-- cookie1,2018-04-16,4


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
-- cookie1,2018-04-10,1,1
-- cookie1,2018-04-11,5,6
-- cookie1,2018-04-12,7,13
-- cookie1,2018-04-13,3,16
-- cookie1,2018-04-14,2,18
-- cookie1,2018-04-15,4,22
-- cookie1,2018-04-16,4,26

-- pv2
-- 分组内从起点到当前行的累积
select cookie_id,
       create_time,
       pv,
       sum(pv)
           over (partition by cookie_id order by create_time rows between unbounded preceding and current row ) as pv2
from big_data_t1;

-- cookie1,2018-04-10,1,1
-- cookie1,2018-04-11,5,6
-- cookie1,2018-04-12,7,13
-- cookie1,2018-04-13,3,16
-- cookie1,2018-04-14,2,18
-- cookie1,2018-04-15,4,22
-- cookie1,2018-04-16,4,26

-- pv3
-- 分组内聚合所有行的pv值
select cookie_id,
       create_time,
       pv,
       sum(pv) over (partition by cookie_id) as pv3
from big_data_t1;

-- cookie1,2018-04-10,1,26
-- cookie1,2018-04-11,5,26
-- cookie1,2018-04-12,7,26
-- cookie1,2018-04-13,3,26
-- cookie1,2018-04-14,2,26
-- cookie1,2018-04-15,4,26
-- cookie1,2018-04-16,4,26

-- pv4
-- 分组内聚合当前行与其前三行的pv值
-- 分组内当前行+往前3行. 如: 11号=10号+11号, 12号=10号+11号+12号, 13号=10号+11号+12号+13号, 14号=11号+12号+13号+14号
select cookie_id,
       create_time,
       pv,
       sum(pv) over (partition by cookie_id order by create_time rows between 3 preceding and current row ) as pv4
from big_data_t1;

-- cookie1,2018-04-10,1,1
-- cookie1,2018-04-11,5,6
-- cookie1,2018-04-12,7,13
-- cookie1,2018-04-13,3,16
-- cookie1,2018-04-14,2,17
-- cookie1,2018-04-15,4,16
-- cookie1,2018-04-16,4,13

-- pv5
-- 分组内当前行+往前3行+往后1行，如，14号=11号+12号+13号+14号+15号=5+7+3+2+4=21
select cookie_id,
       create_time,
       pv,
       sum(pv) over (partition by cookie_id order by create_time rows between 3 preceding and 1 following) as pv5
from big_data_t1;

-- cookie1,2018-04-10,1,6
-- cookie1,2018-04-11,5,13
-- cookie1,2018-04-12,7,16
-- cookie1,2018-04-13,3,18
-- cookie1,2018-04-14,2,21
-- cookie1,2018-04-15,4,20
-- cookie1,2018-04-16,4,13

-- pv6
-- 分组内当前行+往后所有行. 如: 13号=13号+14号+15号+16号=3+2+4+4=13, 14号=14号+15号+16号=2+4+4=10
select cookie_id,
       create_time,
       pv,
       sum(pv)
           over (partition by cookie_id order by create_time rows between current row and unbounded following) as pv6
from big_data_t1;

-- cookie1,2018-04-10,1,26
-- cookie1,2018-04-11,5,25
-- cookie1,2018-04-12,7,20
-- cookie1,2018-04-13,3,13
-- cookie1,2018-04-14,2,10
-- cookie1,2018-04-15,4,8
-- cookie1,2018-04-16,4,4

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
-- cookie1,2018-04-10,1
-- cookie1,2018-04-11,5
-- cookie1,2018-04-12,7
-- cookie1,2018-04-13,3
-- cookie1,2018-04-14,2
-- cookie1,2018-04-15,4
-- cookie1,2018-04-16,4
-- cookie2,2018-04-10,2
-- cookie2,2018-04-11,3
-- cookie2,2018-04-12,5
-- cookie2,2018-04-13,6
-- cookie2,2018-04-14,3
-- cookie2,2018-04-15,9
-- cookie2,2018-04-16,7

-- row_number
-- 从1开始，按照顺序，生成分组内记录的序列
select *,
       row_number() over (partition by cookie_id order by pv desc ) as rn
from big_data_t2;

-- cookie1,2018-04-12,7,1
-- cookie1,2018-04-11,5,2
-- cookie1,2018-04-15,4,3
-- cookie1,2018-04-16,4,4
-- cookie1,2018-04-13,3,5
-- cookie1,2018-04-14,2,6
-- cookie1,2018-04-10,1,7
-- cookie2,2018-04-15,9,1
-- cookie2,2018-04-16,7,2
-- cookie2,2018-04-13,6,3
-- cookie2,2018-04-12,5,4
-- cookie2,2018-04-11,3,5
-- cookie2,2018-04-14,3,6
-- cookie2,2018-04-10,2,7

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

-- cookie1,2018-04-12,7,1,1,1
-- cookie1,2018-04-11,5,2,2,2
-- cookie1,2018-04-15,4,3,3,3
-- cookie1,2018-04-16,4,3,3,4
-- cookie1,2018-04-13,3,5,4,5
-- cookie1,2018-04-14,2,6,5,6
-- cookie1,2018-04-10,1,7,6,7

-- ntile
-- ntile可以看成是：把有序的数据集合平均分配到指定的数量（num）个桶中, 将桶号分配给每一行。如果不能平均分配，则优先分配较小编号的桶，并且各个桶中能放的行数最多相差1。

select *,
       ntile(2) over (partition by cookie_id order by create_time) as ntile_2,
       ntile(3) over (partition by cookie_id order by create_time) as ntile_3,
       ntile(4) over (order by create_time)                        as ntile_4
from big_data_t2
order by cookie_id, create_time;

-- cookie1,2018-04-10,1,1,1,1
-- cookie2,2018-04-10,2,1,1,1
-- cookie1,2018-04-11,5,1,1,1
-- cookie2,2018-04-11,3,1,1,1
-- cookie1,2018-04-12,7,1,1,2
-- cookie2,2018-04-12,5,1,1,2
-- cookie1,2018-04-13,3,1,2,2
-- cookie2,2018-04-13,6,1,2,2
-- cookie1,2018-04-14,2,2,2,3
-- cookie2,2018-04-14,3,2,2,3
-- cookie1,2018-04-15,4,2,3,3
-- cookie2,2018-04-15,9,2,3,4
-- cookie1,2018-04-16,4,2,3,4
-- cookie2,2018-04-16,7,2,3,4

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

-- cookie1,2018-04-10,1,1,1970-01-01 00:00:00,<null>
-- cookie1,2018-04-11,5,2,2018-04-10,<null>
-- cookie1,2018-04-12,7,3,2018-04-11,2018-04-10
-- cookie1,2018-04-13,3,4,2018-04-12,2018-04-11
-- cookie1,2018-04-14,2,5,2018-04-13,2018-04-12
-- cookie1,2018-04-15,4,6,2018-04-14,2018-04-13
-- cookie1,2018-04-16,4,7,2018-04-1hql_first_level5,2018-04-14
-- cookie2,2018-04-10,2,1,1970-01-01 00:00:00,<null>
-- cookie2,2018-04-11,3,2,2018-04-10,<null>
-- cookie2,2018-04-12,5,3,2018-04-11,2018-04-10
-- cookie2,2018-04-13,6,4,2018-04-12,2018-04-11
-- cookie2,2018-04-14,3,5,2018-04-13,2018-04-12
-- cookie2,2018-04-15,9,6,2018-04-14,2018-04-13
-- cookie2,2018-04-16,7,7,2018-04-15,2018-04-14

select *,
       row_number() over (partition by cookie_id order by create_time)                       as rn,
       lead(create_time, 1, '1970-01-01') over (partition by cookie_id order by create_time) as next_1,
       lead(create_time, 2) over (partition by cookie_id order by create_time)               as next_2
from big_data_t2;

-- cookie1,2018-04-10,1,1,2018-04-11,2018-04-12
-- cookie1,2018-04-11,5,2,2018-04-12,2018-04-13
-- cookie1,2018-04-12,7,3,2018-04-13,2018-04-14
-- cookie1,2018-04-13,3,4,2018-04-14,2018-04-15
-- cookie1,2018-04-14,2,5,2018-04-15,2018-04-16
-- cookie1,2018-04-15,4,6,2018-04-16,<null>
-- cookie1,2018-04-16,4,7,1970-01-01,<null>
-- cookie2,2018-04-10,2,1,2018-04-11,2018-04-12
-- cookie2,2018-04-11,3,2,2018-04-12,2018-04-13
-- cookie2,2018-04-12,5,3,2018-04-13,2018-04-14
-- cookie2,2018-04-13,6,4,2018-04-14,2018-04-15
-- cookie2,2018-04-14,3,5,2018-04-15,2018-04-16
-- cookie2,2018-04-15,9,6,2018-04-16,<null>
-- cookie2,2018-04-16,7,7,1970-01-01,<null>

-- first_value: 分组排序后截止到当前行的第一个值
-- last_value: 分组排序后截止到当前行的最后一个值
-- 不指定order by会排序混乱，结果错误

select *,
       row_number() over (partition by cookie_id order by create_time)    as rn,
       first_value(pv) over (partition by cookie_id order by create_time desc) as `first_value`,
       last_value(pv) over (partition by cookie_id order by create_time)  as `last_value`
from big_data_t2;

-- cookie1,2018-04-16,4,7,4,4
-- cookie1,2018-04-15,4,6,4,4
-- cookie1,2018-04-14,2,5,4,2
-- cookie1,2018-04-13,3,4,4,3
-- cookie1,2018-04-12,7,3,4,7
-- cookie1,2018-04-11,5,2,4,5
-- cookie1,2018-04-10,1,1,4,1
-- cookie2,2018-04-16,7,7,7,7
-- cookie2,2018-04-15,9,6,7,9
-- cookie2,2018-04-14,3,5,7,3
-- cookie2,2018-04-13,6,4,7,6
-- cookie2,2018-04-12,5,3,7,5
-- cookie2,2018-04-11,3,2,7,3
-- cookie2,2018-04-10,2,1,7,2

