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
-- /*********************【2. 排名中取他值】**********************/
-- /*********************【2. 排名中取他值】**********************/
-- /*********************【2. 排名中取他值】**********************/
-- /*********************【2. 排名中取他值】**********************/
-- /*********************【2. 排名中取他值】**********************/
-- /*********************【2. 排名中取他值】**********************/
-- /*********************【2. 排名中取他值】**********************/
-- /*********************【2. 排名中取他值】**********************/
