-- 本文目录：
-- 一、行列转换
-- 二、排名中取他值
-- 三、累计求值
-- 四、窗口大小控制
-- 五、产生连续数值
-- 六、数据扩充与收缩
-- 七、合并与拆分
-- 八、模拟循环操作
-- 九、不使用distinct或group by去重
-- 十、容器--反转内容
-- 十一、多容器--成对提取数据
-- 十二、多容器--转多行
-- 十三、抽象分组--断点排序
-- 十四、业务逻辑的分类与抽象--时效
-- 十五、时间序列--进度及剩余
-- 十六、时间序列--构造日期
-- 十七、时间序列--构造累积日期
-- 十八、时间序列--构造连续日期
-- 十九、时间序列--取多个字段最新的值
-- 二十、时间序列--补全数据
-- 二十一、时间序列--取最新完成状态的前一个状态
-- 二十二、非等值连接--范围匹配
-- 二十三、非等值连接--最近匹配
-- 二十四、N指标--累计去重

-- /*********************【1. 行列转换】**********************/
-- 表中记录了各年份各部门的平均绩效考核成绩。
create table t1
(
    year        string comment '年份',
    dept_id     string comment '部门',
    performance int comment '绩效得分'
) comment '各年份各部门的平均绩效考核表'
    row format delimited fields terminated by '\t'
    stored as orc
    location '/warehouse/test/t1';

insert into table t1
values ('2014', 'B', 9),
       ('2015', 'A', 8),
       ('2014', 'A', 10),
       ('2015', 'B', 7);

select *
from t1;

-- 2014,    B,    9
-- 2015,    A,    8
-- 2014,    A,    10
-- 2015,    B,    7

-- (1) 多行转多列
select year,
       max(case dept_id when 'A' then performance end) as `A`,
       max(case dept_id when 'B' then performance end) as `B`
from t1
group by year;

-- 2015,    8,     7
-- 2014,    10,    9

-- (2) 将上面的结果表转成源表: 多列转多行
with table_temp as (
    select year,
           max(case dept_id when 'A' then performance end) as `A`,
           max(case dept_id when 'B' then performance end) as `B`
    from t1
    group by year
)
select year,
       'A' as dept_id,
       `A` as performance
from table_temp
union all
select year,
       'B' as dept_id,
       `B` as performance
from table_temp
order by year, dept_id;

-- 2014,    A,    10
-- 2014,    B,    9
-- 2015,    A,    8
-- 2015,    B,    7

create table t2
(
    year        string comment '年份',
    dept_id     string comment '部门',
    performance int comment '绩效得分'
) comment '各年份各部门的平均绩效考核表'
    row format delimited fields terminated by '\t'
    stored as orc
    location '/warehouse/test/t2';

insert into table t2
values ('2014', 'B', 9),
       ('2015', 'A', 8),
       ('2014', 'A', 10),
       ('2015', 'B', 7),
       ('2014', 'B', 6);

select *
from t2;

-- 2014,    B,    9
-- 2015,    A,    8
-- 2014,    A,    10
-- 2015,    B,    7
-- 2014,    B,    6

-- (3) 解释: 同一个部门会有多个绩效, 求多行转多列结果
select year,
       max(case dept_id when 'A' then performance_set end) as `A`,
       max(case dept_id when 'B' then performance_set end) as `B`
from (
         select year,
                dept_id,
                concat_ws(',', collect_set(cast(performance as string))) as performance_set
         from t2
         group by year, dept_id
     ) as table_temp
group by year;

-- 2015,    8,    7
-- 2014,    10,   9,6

-- /*********************【2. 排名中取他值】**********************/
create table t3
(
    year        string comment '年份',
    dept_id     string comment '部门',
    performance int comment '绩效得分'
) comment '各年份各部门的平均绩效考核表'
    row format delimited fields terminated by '\t'
    stored as orc
    location '/warehouse/test/t3';

insert into table t3
values ('2014', 'A', 3),
       ('2014', 'B', 1),
       ('2014', 'C', 2),
       ('2015', 'A', 4),
       ('2015', 'D', 3);

select *
from t3;

-->a字段     b字段  c字段
-- 2014,    A,    3
-- 2014,    B,    1
-- 2014,    C,    2
-- 2015,    A,    4
-- 2015,    D,    3

set hive.execution.engine = spark;

select *
from t3;

select *,
       sum(performance) over (partition by year order by dept_id) as test
from t3;

select *,
       sum(performance)
           over (partition by year order by dept_id rows between unbounded preceding and current row ) as test
from t3;

-- (1) 按a分组取b字段最小时对应的c字段
select year,
       performance
from (
         select *,
                row_number() over (partition by year order by dept_id) as rn
         from t3
     ) as table_temp
where rn = 1
order by year;

-- 2014,    3
-- 2015,    4

-- (2) 按a分组取b字段排第二时对应的c字段
select year,
       performance
from (
         select *,
                row_number() over (partition by year order by dept_id) as rn
         from t3
     ) as table_temp
where rn = 2
order by year;

-- 2014,    1
-- 2015,    3

-- (3) 按a分组取b字段最小和最大时对应的c字段
select year,
       sum(`if`(asc_rn = 1, performance, 0))  as min_performance,
       sum(`if`(desc_rn = 1, performance, 0)) as max_performance
from (
         select *,
                row_number() over (partition by year order by dept_id)      as asc_rn,
                row_number() over (partition by year order by dept_id desc) as desc_rn
         from t3
     ) as table_temp
where asc_rn = 1
   or desc_rn = 1
group by year
order by year;

-- 2014,    3,    2
-- 2015,    4,    3

-- (4) 按a分组取b字段第二小和第二大时对应的c字段
select year,
       sum(`if`(asc_rn = 2, performance, 0))  as second_min_performance,
       sum(`if`(desc_rn = 2, performance, 0)) as second_max_performance
from (
         select *,
                row_number() over (partition by year order by dept_id)      as asc_rn,
                row_number() over (partition by year order by dept_id desc) as desc_rn
         from t3
     ) as table_temp
where asc_rn = 2
   or desc_rn = 2
group by year
order by year;

-- 2014,    1,    1
-- 2015,    3,    4

-- (5) 按a分组取b字段前两小和前两大时对应的c字段
with a as (
    select *,
           row_number() over (partition by year order by dept_id)      as asc_rn,
           row_number() over (partition by year order by dept_id desc) as desc_rn
    from t3
),
     b as (
         select year,
                concat_ws(',', collect_set(cast(performance as string))) as min_performance
         from a
         where asc_rn <= 2
         group by year
     ),
     c as (
         select year,
                concat_ws(',', collect_set(cast(performance as string))) as max_performance
         from a
         where desc_rn <= 2
         group by year
     )
select b.year,
       min_performance,
       max_performance
from b
         left join c on b.year = c.year
order by year;

-- 2014,    3,1,    2,1
-- 2015,    4,3,    3,4


-- /*********************【3. 累计求值】**********************/
-- (1) 按a分组按b字段排序，对c累计求和
select *,
       row_number() over (partition by year order by dept_id)     as rn,
       sum(performance) over (partition by year order by dept_id) as cumulative_sum
from t3;

-- 2015,    A,    4,    1,    4
-- 2015,    D,    3,    2,    7
-- 2014,    A,    3,    1,    3
-- 2014,    B,    1,    2,    4
-- 2014,    C,    2,    3,    6

-- (2) 按a分组按b字段排序，对c取累计平均值
select *,
       row_number() over (partition by year order by dept_id)     as rn,
       avg(performance) over (partition by year order by dept_id) as cumulative_avg
from t3;

-- 2015,    A,    4,    1,    4
-- 2015,    D,    3,    2,    3.5
-- 2014,    A,    3,    1,    3
-- 2014,    B,    1,    2,    2
-- 2014,    C,    2,    3,    2

-- (3) 按a分组按b字段排序，对b取累计排名比例
select *,
       row_number() over (partition by year order by dept_id) as rn,
       round((row_number() over (partition by year order by dept_id)) / (count(*) over (partition by year)),
             2)                                               as cumulative_row_number_percent
from t3;

-- 2014,    A,    3,    1,    0.33
-- 2014,    B,    1,    2,    0.67
-- 2014,    C,    2,    3,    1
-- 2015,    A,    4,    1,    0.5
-- 2015,    D,    3,    2,    1

-- 穿插concat与concat_ws用法区别
-- +----+-------+--------+
-- | id | name1 | name2  |
-- +----+-------+--------+
-- | 1  | Bob   | Smith  |
-- | 2  | Jane  | Doe    |
-- | 3  | John  | Smith  |
-- +----+-------+--------+

SELECT concat(name1, name2) as name
FROM my_table;

-- +----------+
-- |   name   |
-- +----------+
-- | BobSmith |
-- | JaneDoe  |
-- | JohnSmith|
-- +----------+

SELECT concat_ws(' ', name1, name2) as name
FROM my_table;

-- +-----------+
-- |    name   |
-- +-----------+
-- | Bob Smith |
-- | Jane Doe  |
-- | John Smith|
-- +-----------+

-- (4) 按a分组按b字段排序，对b取累计求和比例
select *,
       row_number() over (partition by year order by dept_id) as rn,
       round((sum(performance) over (partition by year order by dept_id)) / (sum(performance) over (partition by year)),
             2)                                               as cumulative_sum_percent
from t3;

-- 2014,    A,    3,    1,    0.5
-- 2014,    B,    1,    2,    0.67
-- 2014,    C,    2,    3,    1
-- 2015,    A,    4,    1,    0.57
-- 2015,    D,    3,    2,    1


-- /*********************【4. 窗口大小控制】**********************/
-- (1) 按a分组按b字段排序，对c取前后各一行的和
select *,
       lag(performance, 1, 0) over (partition by year order by dept_id) +
       lead(performance, 1, 0) over (partition by year order by dept_id) as sum_of_pre_and_following
from t3;

-- 2015,    A,    4,    3
-- 2015,    D,    3,    4
-- 2014,    A,    3,    1
-- 2014,    B,    1,    5
-- 2014,    C,    2,    1

-- (2) 按a分组按b字段排序，对c取平均值，注意: 取前一行与当前行的均值！
select *,
       round(avg(performance) over (partition by year order by dept_id rows between 1 preceding and current row ),
             2) as avg_of_pre_and_current
from t3;

-- 2015,    A,    4,    4
-- 2015,    D,    3,    3.5
-- 2014,    A,    3,    3
-- 2014,    B,    1,    2
-- 2014,    C,    2,    1.5

-- 方式二
select year,
       dept_id,
       case
           when pre_performance is null then performance
           else round((pre_performance + performance) / 2, 2) end as avg_of_pre_and_current
from (
         select *,
                lag(performance, 1) over (partition by year order by dept_id) as pre_performance
         from t3
     ) as table_temp;

-- 2015,    A,    4
-- 2015,    D,    3.5
-- 2014,    A,    3
-- 2014,    B,    2
-- 2014,    C,    1.5


-- /*********************【5. 产生连续数值】**********************/
-- 输出结果
-- 1
-- 2
-- 3
-- 4
-- 5
-- ...
-- 100

-- (1) 不借助其他任何外表，实现产生连续数值
-- 方式一: 产生一百万行, 1 ~ 100,0000
select id_start + pos as id
from (
         select 1       as id_start,
                1000000 as id_end
     ) m lateral view posexplode(split(space(id_end - id_start), '')) t as pos, val;

-- 方式二:
select row_number() over () as id
from (select split(space(99), '') as x) t
         lateral view
             explode(x) ex;

select split(space(3), ' ') as x;
-- ["","","",""]
select split(space(3), '') as x;
-- [" "," "," ",""]


-- /*********************【6. 数据扩充与收缩】**********************/
create table t4
(
    a string
) comment ''
    row format delimited fields terminated by '\t'
    stored as orc
    location '/warehouse/test/t4';

insert into table t4
values (3),
       (2),
       (4);

select *
from t4;

-- (1) 数据扩充
-- a   b
-- 3   3、2、1
-- 2   2、1
-- 4   4、3、2、1

select t.a,
       concat_ws('、', collect_set(cast(t.rn as string))) as b
from (
         select t4.a,
                b.rn
         from t4
                  left join (
             select row_number() over () as rn
             from (select split(space(5), '') as x) t -- space(5)可根据t4表的最大值灵活调整
                      lateral view explode(x) pe
         ) b on 1 = 1
         where t4.a >= b.rn
         order by t4.a, b.rn desc
     ) t
group by t.a;

-- 3,    1、3、2
-- 4,    1、4、3、2
-- 2,    1、2

-- (2) 数据扩充，排除偶数
-- a   b
-- 3   3、1
-- 2   1
-- 4   3、1

select t.a,
       concat_ws('、', collect_set(cast(t.rn as string))) as b
from (
         select t4.a,
                b.rn
         from t4
                  left join
              (
                  select row_number() over () as rn
                  from (select split(space(5), ' ') as x) t
                           lateral view
                               explode(x) pe
              ) b
              on 1 = 1
         where t4.a >= b.rn
           and b.rn % 2 = 1
         order by t4.a, b.rn desc
     ) t
group by t.a;

-- (3) 如何处理字符串累计拼接, 将小于等于a字段的值聚合拼接起来
SELECT t.a
     , concat_ws('、', collect_set(cast(t.a1 AS string))) AS b
FROM (
         SELECT t4.a
              , b.a1
         FROM t4
                  LEFT JOIN
              (
                  SELECT a AS a1
                  FROM t4
              ) b
              ON 1 = 1
         WHERE t4.a >= b.a1
         ORDER BY t4.a, b.a1
     ) t
GROUP BY t.a;

-- (4) 如果a字段有重复，如何实现字符串累计拼接
create table t5
(
    a string
) comment ''
    row format delimited fields terminated by '\t'
    stored as orc
    location '/warehouse/test/t5';

insert into table t5
values (2),
       (3),
       (3),
       (4);

select *
from t5;

with x as (
    select a,
           row_number() over (order by a) as rn
    from t5
),
     y as (
         select x_1.a,
                x_1.rn,
                x_2.a as b
         from x as x_1
                  left join x as x_2 on x_1.rn >= x_2.rn
     )
select a,
       concat_ws('、', collect_list(b)) as a_list
from y
group by a, rn
order by a, a_list;

-- 答案: 跟自己写的思路是一样的
select a,
       b
from (
         select t.a,
                t.rn,
                concat_ws('、', collect_list(cast(t.a1 as string))) as b
         from (
                  select a.a,
                         a.rn,
                         b.a1
                  from (
                           select a,
                                  row_number() over (order by a ) as rn
                           from t5
                       ) a
                           left join
                       (
                           select a                               as a1,
                                  row_number() over (order by a ) as rn
                           from t5
                       ) b
                       on 1 = 1
                  where a.a >= b.a1
                    and a.rn >= b.rn
                  order by a.a, b.a1
              ) t
         group by t.a, t.rn
         order by t.a, t.rn
     ) tt;

-- (5) 数据展开: 如何将字符串"1-5,16,11-13,9"扩展成"1,2,3,4,5,16,11,12,13,9"？注意顺序不变。
select concat_ws(',', collect_list(cast(rn as string)))
from (
         select a.rn,
                b.num,
                b.pos
         from (
                  select row_number() over () as rn
                  from (select split(space(20), ' ') as x) t -- space(20)可灵活调整
                           lateral view
                               explode(x) pe
              ) a lateral view outer
             posexplode(split('1-5,16,11-13,9', ',')) b as pos, num
         where a.rn between cast(split(num, '-')[0] as int) and cast(split(num, '-')[1] as int)
            or a.rn = num
         order by pos, rn
     ) t;


-- /*********************【7. 合并与拆分】**********************/
create table t6
(
    a string,
    b string
) comment ''
    row format delimited fields terminated by '\t'
    stored as orc
    location '/warehouse/test/t6';

insert into table t6
values ('2014', 'A'),
       ('2014', 'B'),
       ('2015', 'B'),
       ('2015', 'D');

select *
from t6;

-- (1) 合并
select a,
       concat_ws('、', collect_list(b)) as b_list
from t6
group by a
order by a;

-- 2014,    A、B
-- 2015,    B、D

-- (2) 拆分: 将分组合并的结果拆分出来
with temp_a as (
    select a,
           concat_ws('、', collect_list(b)) as b_list
    from t6
    group by a
)
select a, b
from temp_a lateral view explode(split(temp_a.b_list, '、')) temp_b as b
order by a;

-- 2014,    A
-- 2014,    B
-- 2015,    B
-- 2015,    D


-- /*********************【8. 模拟循环操作】**********************/
create table t7
(
    a string
) comment ''
    row format delimited fields terminated by '\t'
    stored as orc
    location '/warehouse/test/t7';

insert into table t7
values ('1011'),
       ('0101');

select *
from t7;

-- (1) 如何将字符'1'的位置提取出来
select a,
       concat_ws(",", collect_list(cast(index as string))) as res
from (
         select a,
                index + 1 as index,
                chr
         from (
                  select a,
                         concat_ws(",", substr(a, 1, 1), substr(a, 2, 1), substr(a, 3, 1), substr(a, -1)) str
                  from t7
              ) tmp1
                  lateral view posexplode(split(str, ",")) t as index, chr
         where chr = "1"
     ) tmp2
group by a;


-- /*********************【9. 不使用distinct或group by去重】**********************/
create table t8
(
    a string,
    b string,
    c string,
    d string
) comment ''
    row format delimited fields terminated by '\t'
    stored as orc
    location '/warehouse/test/t8'
    tblproperties ('orc.compress' = 'snappy');

insert into table t8
values ('2014', '2016', '2014', 'A'),
       ('2014', '2015', '2015', 'B');

select *
from t8;

-- 2014,    2016,    2014,    A
-- 2014,    2015,    2015,    B

-- (1) 不使用distinct或group by去重
with temp_a as (
    select a as year,
           d as dept
    from t8
    union all
    select b as year,
           d as dept
    from t8
    union all
    select c as year,
           d as dept
    from t8
)
select year,
       dept
from (
         select *,
                row_number() over (partition by year, dept) as rn
         from temp_a
     ) as temp_b
where rn = 1
order by dept;

-- 2016,    A
-- 2014,    A
-- 2014,    B
-- 2015,    B


-- /*********************【10. 容器--反转内容】**********************/
create table t9
(
    a string
) comment ''
    row format delimited fields terminated by '\t'
    stored as orc
    location '/warehouse/test/t9'
    tblproperties ('orc.compress' = 'snappy');

insert into table t9
values ('AB,CA,BAD'),
       ('BD,EA');

select *
from t9;

-- (1) 反转逗号分隔的数据：改变顺序，内容不变
select a
from (
         select concat_ws(',', collect_list(element) over (partition by a order by pos desc)) as a,
                pos
         from (
                  select a,
                         pos,
                         element
                  from t9
                           lateral view posexplode(split(a, ',')) temp_a as pos, element
              ) as temp_b
     ) as temp_c
where pos = 0;

-- BAD,CA,AB
-- EA,BD


-- 答案: 使用了reverse函数
select a,
       concat_ws(",", collect_list(reverse(str)))
from (
         select a,
                str
         from t9
                  lateral view explode(split(reverse(a), ",")) t as str
     ) tmp1
group by a;

-- (2) 反转逗号分隔的数据：改变内容，顺序不变
select a, concat_ws(',', collect_list(reverse(str))) as reverse_a
from (
         select a,
                str
         from t9
                  lateral view explode(split(a, ',')) temp_a as str
     ) as temp_b
group by a;


-- AB,CA,BAD,    BA,AC,DAB
-- BD,EA,        DB,AE


-- /*********************【11. 多容器--成对提取数据】**********************/
create table t10
(
    a string,
    b string
) comment ''
    row format delimited fields terminated by '\t'
    stored as orc
    location '/warehouse/test/t10'
    tblproperties ('orc.compress' = 'snappy');

insert into table t10
values ('A/B', '1/3'),
       ('B/C/D', '4/5/2');

select *
from t10;

-- (1) 成对提取数据，字段一一对应
select team,
       score
from t10
         lateral view posexplode(split(a, '/')) temp_a as pos_team, team
         lateral view posexplode(split(b, '/')) temp_b as pos_score, score
where pos_team = pos_score;

-- A,    1
-- B,    3
-- B,    4
-- C,    5
-- D,    2


-- /*********************【12. 多容器--转多行】**********************/
create table t11
(
    a string,
    b string,
    c string
) comment ''
    row format delimited fields terminated by '\t'
    stored as orc
    location '/warehouse/test/t11'
    tblproperties ('orc.compress' = 'snappy');

insert into table t11
values ('001', 'A/B', '1/3/5'),
       ('002', 'B/C/D', '4/5');

select *
from t11;

-- (1) 转多行
select a,
       'type_b' as d,
       e
from t11
         lateral view explode(split(b, '/')) temp_a as e
union all
select a,
       'type_c' as d,
       e
from t11
         lateral view explode(split(c, '/')) temp_a as e
order by a, d;


-- 001,    type_b,    A
-- 001,    type_b,    B
-- 001,    type_c,    1
-- 001,    type_c,    3
-- 001,    type_c,    5
-- 002,    type_b,    B
-- 002,    type_b,    C
-- 002,    type_b,    D
-- 002,    type_c,    4
-- 002,    type_c,    5


-- /*********************【13. 抽象分组--断点排序】**********************/
create table t12
(
    a string,
    b int
) comment ''
    row format delimited fields terminated by '\t'
    stored as orc
    location '/warehouse/test/t12'
    tblproperties ('orc.compress' = 'snappy');

insert into table t12
values ('2014', 1),
       ('2015', 1),
       ('2016', 1),
       ('2017', 0),
       ('2018', 0),
       ('2019', -1),
       ('2020', -1),
       ('2021', -1),
       ('2022', 1),
       ('2023', 1);

select *
from t12;

-- (1) 断点排序
select a,
       b,
       row_number() over (partition by b, flag) as c
from (
         select a,
                b,
                sum(flag) over (partition by b order by a rows between unbounded preceding and current row ) as flag
         from (
                  select *,
                         `if`(cast(a as int) - cast(lag(a, 1) over (partition by b order by a) as int) = 1, 0,
                              1) as flag
                  from t12
              ) as temp_a
     ) as temp_b;

-- 需要纠正: (两处区别)
-- 2019,    -1,    1
-- 2020,    -1,    0
-- 2021,    -1,    0
-- 2017,    0,     1
-- 2018,    0,     0
-- 2014,    1,     1
-- 2015,    1,     0
-- 2016,    1,     0
-- 2022,    1,     1
-- 2023,    1,     0


set hive.execution.engine = spark;

with temp_a as (
    select *,
           `if`(cast(a as int) - cast(lag(a, 1) over (partition by b order by a ) as int) = 1, 0,
                1) as flag
    from t12
)
select a,
       b,
       sum(flag) over (partition by b order by a rows between unbounded preceding and current row ) as flag
from temp_a;

with temp_a as (
    select 1 as a
    union all
    select 2 as a
    union all
    select 3 as a
    union all
    select 4 as a
    union all
    select 5 as a
),
     temp_b as (
         select null as a
         union all
         select 2 as a
         union all
         select 3 as a
         union all
         select 4 as a
         union all
         select 5 as a
     ),
     temp_c as (
         select "2" as a
         union all
         select "2" as a
         union all
         select "1" as a
         union all
         select "1" as a
         union all
         select "1" as a
     )
select sum(a)
from temp_c;

-- spark
-- 2019,    -1,    1
-- 2020,    -1,    2
-- 2021,    -1,    3
-- 2017,    0,     1
-- 2018,    0,     2
-- 2014,    1,     1
-- 2015,    1,     2
-- 2016,    1,     3
-- 2022,    1,     4
-- 2023,    1,     5

-- mr
-- 2019,    -1,    1
-- 2020,    -1,    1
-- 2021,    -1,    1
-- 2017,    0,     1
-- 2018,    0,     1
-- 2014,    1,     1
-- 2015,    1,     1
-- 2016,    1,     1
-- 2022,    1,     2
-- 2023,    1,     2


select a,
       b,
       sum(flag) over (partition by b order by a rows between unbounded preceding and current row ) as flag
from (
         select *,
                `if`(cast(a as int) - cast(lag(a, 1) over (partition by b order by a) as int) = 1, 0,
                     1) as flag
         from t12
     ) as temp_a;

-- spark
-- 2019,    -1,    1
-- 2020,    -1,    1
-- 2021,    -1,    1
-- 2017,    0,     1
-- 2018,    0,     1
-- 2014,    1,     1
-- 2015,    1,     1
-- 2016,    1,     1
-- 2022,    1,     2
-- 2023,    1,     2

-- mr
-- 2019,    -1,    1
-- 2020,    -1,    1
-- 2021,    -1,    1
-- 2017,    0,     1
-- 2018,    0,     1
-- 2014,    1,     1
-- 2015,    1,     1
-- 2016,    1,     1
-- 2022,    1,     2
-- 2023,    1,     2

-- 答案:
select a,
       b,
       row_number() over ( partition by b,repair_a order by a asc) as c--按照b列和[b的组首]分组，排序
from (
         select a,
                b,
                a - b_rn as repair_a--根据b列值出现的次序,修复a列值为b首次出现的a列值,称为b的[组首]
         from (
                  select a,
                         b,
                         row_number() over ( partition by b order by a asc ) as b_rn--按b列分组,按a列排序,得到b列各值出现的次序
                  from t12
              ) tmp1
     ) tmp2--注意，如果不同的b列值，可能出现同样的组首值，但组首值需要和a列值 一并参与分组，故并不影响排序。
order by a asc;


-- /*********************【14. 业务逻辑的分类与抽象--时效】**********************/
create table t13
(
    date_id string,
    is_work string
) comment '日期表'
    row format delimited fields terminated by '\t'
    stored as orc
    location '/warehouse/test/t13'
    tblproperties ('orc.compress' = 'snappy');

insert into table t13
values ('2017-04-13', 1),
       ('2017-04-14', 1),
       ('2017-04-15', 0),
       ('2017-04-16', 0),
       ('2017-04-17', 1);

select *
from t13;


create table t14
(
    a string,
    b string,
    c string
) comment '客户申请表'
    row format delimited fields terminated by '\t'
    stored as orc
    location '/warehouse/test/t14'
    tblproperties ('orc.compress' = 'snappy');

insert into table t14
values ('1', '申请', '2017-04-14 18:03:00'),
       ('1', '通过', '2017-04-17 09:43:00'),
       ('2', '申请', '2017-04-13 17:02:00'),
       ('2', '通过', '2017-04-15 09:42:00');

select *
from t14;


-- 2017-04-13,    1
-- 2017-04-14,    1
-- 2017-04-15,    0
-- 2017-04-16,    0
-- 2017-04-17,    1

-- 1,    申请,    2017-04-14 18:03:00
-- 1,    通过,    2017-04-17 09:43:00
-- 2,    申请,    2017-04-13 17:02:00
-- 2,    通过,    2017-04-15 09:42:00


-- 工作日：周一至周五09:30-18:30
-- (1) 计算上表中从申请到通过占用的工作时长
select a,
       max(case b when '申请' then c end) as apply_time,
       max(case )
from t14
group by a;



SELECT a
     , MAX(case WHEN b = '申请' THEN c end) apply_time
     , MAX(case WHEN b = '通过' THEN c end) pass_time
FROM t14
GROUP BY a



-- /*********************【2. 排名中取他值】**********************/
-- /*********************【2. 排名中取他值】**********************/
-- /*********************【2. 排名中取他值】**********************/
-- /*********************【2. 排名中取他值】**********************/
-- /*********************【2. 排名中取他值】**********************/
-- /*********************【2. 排名中取他值】**********************/
-- /*********************【2. 排名中取他值】**********************/
-- /*********************【2. 排名中取他值】**********************/
-- /*********************【2. 排名中取他值】**********************/
-- /*********************【2. 排名中取他值】**********************/
