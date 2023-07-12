-- 查询引擎指定为spark
SET hive.execution.engine=spark;

-- 创建学生表
use firstlevel;

use hql_first_level;

drop table if exists student;

create table if not exists student
(
    stu_id   string comment '学生id',
    stu_name string comment '学生姓名',
    birthday date comment '出生日期',
    sex      string comment '性别'
) row format delimited fields terminated by ','
    stored as textfile;

-- 创建课程表
drop table if exists course;

create table if not exists course
(
    course_id   string comment '课程id',
    course_name string comment '课程名',
    tea_id      string comment '任课老师id'
) row format delimited fields terminated by ','
    stored as textfile;

-- 创建老师表
drop table if exists teacher;

create table if not exists teacher
(
    tea_id   string comment '老师id',
    tea_name string comment '老师姓名'
) row format delimited fields terminated by ','
    stored as textfile;

--创建分数表
drop table if exists score;

create table if not exists score
(
    stu_id    string comment '学生id',
    course_id string comment '课程id',
    score     int comment '成绩'
) row format delimited fields terminated by ','
    stored as textfile;

/*
    上传4个文件数据：

    1.score.txt
    2.teacher.txt
    3.course.txt
    4.student.txt
 */

load data local inpath '/opt/module/datas/score.txt' into table score;

load data local inpath '/opt/module/datas/teacher.txt' into table teacher;

load data local inpath '/opt/module/datas/course.txt' into table course;

load data local inpath '/opt/module/datas/student.txt' into table student;

select *
from course;

select *
from teacher;

select *
from course;

select *
from student;

-- 1
select *
from student
where stu_name like '%冰%';

-- 2
select count(*)
from teacher
where tea_name like '王%';

-- 3
select stu_id, course_id, score
from score
where course_id = '04'
  and score < 60
order by score desc;

-- 4
select s.stu_id, stu_name, course_name, score
from score s
         left join course c on s.course_id = c.course_id
         left join student st on s.stu_id = st.stu_id
where course_name = '数学'
  and score < 60
order by stu_id;

-- 5
with a as (
    select stu_id,
           stu_name,
           date_format(birthday, 'yyyy')         as year_flag,
           date_format(birthday, 'MM')           as month_flag,
           date_format(`current_date`(), 'yyyy') as curr_year,
           date_format(`current_date`(), 'MM')   as curr_month
    from student
),
     b as (
         select stu_id,
                stu_name,
                cast(curr_year as bigint) - cast(year_flag as bigint)   as year_flag,
                cast(curr_month as bigint) - cast(month_flag as bigint) as month_flag
         from a
     )
select stu_id,
       stu_name,
       concat(`if`(month_flag < 0, cast((year_flag - 1) as string), cast(year_flag as string)), '岁余',
              `if`(month_flag < 0, cast((12 + month_flag) as string), cast(month_flag as string)), '个月') as age
from b;

-- 方式2
with a as (
    select stu_id,
           stu_name,
           year(`current_date`()) - year(birthday)   as year_flag,
           month(`current_date`()) - month(birthday) as month_flag
    from student
)
select stu_name,
       concat(`if`(month_flag < 0, year_flag - 1, year_flag), '岁余',
              `if`(month_flag < 0, 12 + month_flag, month_flag), '个月') as age
from a;

-- 6
select *
from student;

select stu_name
from student
where month(birthday) = month(`current_date`());

-- 7
select course_id,
       sum(score) as total_score
from score
where course_id = '02'
group by course_id;

-- 8
select count(distinct stu_id)
from score;

-- 9
select course_id,
       min(score) as min_score,
       max(score) as max_score
from score
group by course_id
order by course_id;

-- 10
select course_id,
       count(distinct stu_id) as cnt
from score
group by course_id
order by course_id;

-- 11
select sex,
       count(*) as num
from student
group by sex;

-- 12
select stu_id,
       avg(score) as avg_score
from score
group by stu_id
having avg_score > 60
order by stu_id;

-- 13
select stu_id
from score
group by stu_id
having count(course_id) >= 2
order by stu_id;

-- 14
select substr(stu_name, 0, 1) as surname,
       collect_set(stu_name)  as name_set,
       count(*)               as num
from student
group by substr(stu_name, 0, 1)
having num > 1
order by surname;

-- 15
select course_id,
       avg(score) as avg_score
from score
group by course_id
order by avg_score, course_id desc;

-- 16
select course_id,
       count(*) as num
from score
group by course_id
having count(*) >= 15
order by course_id;

-- 17
select stu_id,
       total_score,
       dense_rank() over (order by total_score desc) as rk
from (
         select stu_id,
                sum(score) as total_score
         from score
         group by stu_id
     ) as table_tmp
order by rk;

-- 18
select stu_id,
       avg(score) as avg_score
from score
group by stu_id
having avg_score > 60
order by stu_id;

-- 19
select student.stu_id                        as `学生id`,
       stu_name                              as `学生姓名`,
       sum(`if`(course_id = '01', score, 0)) as `语文`,
       sum(`if`(course_id = '02', score, 0)) as `数学`,
       sum(`if`(course_id = '03', score, 0)) as `英语`,
       count(course_id)                      as `有效课程数`,
       nvl(avg(score), 0)                    as `有效平均成绩`
from student
         left join score on score.stu_id = student.stu_id
group by student.stu_id, stu_name
order by student.stu_id;

-- 20
select student.stu_id,
       stu_name,
       count(course_id) as course_cnt
from score
         left join student on score.stu_id = student.stu_id
group by student.stu_id, stu_name
having count(*) = 2
   and (max(course_id) = '02' or min(course_id) = '01')
order by student.stu_id;

-- 21
select student.stu_id,
       stu_name,
       count(*) as course_cnt
from score
         left join student on score.stu_id = student.stu_id
group by student.stu_id, stu_name
having count(*) = 3
   and student.stu_id in (select score.stu_id from score where course_id = '01')
order by student.stu_id;

-- 22
select stu_id,
       stu_name
from (
         select student.stu_id,
                stu_name,
                `if`(score < 60 or score is null, 0, 1) as flag
         from student
                  left join score on score.stu_id = student.stu_id
     ) as table_temp
group by stu_id, stu_name
having sum(flag) = 0
order by stu_id;

-- 23
select student.stu_id,
       stu_name
from student
         left join score on student.stu_id = score.stu_id
group by student.stu_id, stu_name
having count(distinct course_id) <> (select count(distinct course_id) from score)
order by student.stu_id;

-- 24
select student.stu_id,
       stu_name,
       count(course_id) as course_cnt
from score
         left join student on score.stu_id = student.stu_id
group by student.stu_id, stu_name
having count(course_id) = 2
order by student.stu_id;

-- 25
select stu_id,
       stu_name
from student
where stu_id in (
    select stu_id
    from score
    group by stu_id
    having count(1) = 2
)
order by stu_id;

-- 26
select stu_id,
       stu_name
from student
where substr(birthday, 0, 4) = '1990'
order by stu_id;

-- 27
select stu_id,
       stu_name
from student
where year(birthday) = 1990
order by stu_id;

-- 28
select stu_id,
       stu_name
from student
where date_format(birthday, 'yyyy') = 1990
order by stu_id;

-- 29
select *
from course;

-- 30
create table if not exists test
(
    num  bigint comment '数值',
    name string comment '名字'
) comment '测试用表'
    row format delimited fields terminated by '\t'
    stored as orc
    location '/warehouse/test/test'
    tblproperties ('orc.compress' = 'snappy');

insert into table hql_first_level.test
values (12, '十二'),
       (13, '十三'),
       (14, '十四'),
       (15, '十五');

select *
from test;

select sum(num) as num_total
from test
group by 'a';


select *
from test;

-- 31
create table if not exists test_two
(
    num   bigint comment '数值',
    name  string comment '名字',
    alias string comment '小名'
) comment '测试用表'
    row format delimited fields terminated by '\t'
    stored as orc
    location '/warehouse/test/test_two'
    tblproperties ('orc.compress' = 'snappy');

insert into table hql_first_level.test_two
values (12, '十二', '十二'),
       (13, '十三', '十三'),
       (14, '十四', '十四'),
       (15, '十五', '十五');
insert into table hql_first_level.test_two
values (12, '十二', '十二三'),
       (13, '十三', '十三四'),
       (14, '十四', '十四五'),
       (15, '十五', '十五六');
insert into table hql_first_level.test_two
values (12, '十三', '十二三'),
       (13, '十四', '十三四'),
       (14, '十五', '十四五'),
       (15, '十六', '十五六');
insert into table hql_first_level.test_two
values (12, '十三', '十三二'),
       (13, '十四', '十四三'),
       (14, '十五', '十五四'),
       (15, '十六', '十六五');

select *
from test_two;

select num,
       count() over () as cnt
from test_two
group by num
order by num;

select num,
       count() over (partition by num) as cnt
from test_two
group by num, name
order by num;

-- 32
select stu_id,
       stu_name,
       (cast(avg_score as decimal(4, 2))) as avg_score
from (
         select student.stu_id,
                stu_name,
                `if`(score is null or score < 60, 1, 0)               as flag,
                avg(nvl(score, 0)) over (partition by student.stu_id) as avg_score
         from student
                  left join score on student.stu_id = score.stu_id
     ) as table_temp
group by stu_id, stu_name, avg_score
having sum(flag) > 1
order by stu_id;

select cast(0.6666 as decimal(4, 2)) as test;

-- 33
select student.stu_id,
       stu_name,
       count(distinct course_id) as course_cnt,
       sum(nvl(score, 0))        as total_score
from student
         left join score on student.stu_id = score.stu_id
group by student.stu_id, stu_name
order by student.stu_id;

-- 34
create table if not exists test_three
(
    num   bigint comment '数值',
    name  string comment '名字',
    alias string comment '小名'
) comment '测试用表'
    row format delimited fields terminated by '\t'
    stored as orc
    location '/warehouse/test/test_three'
    tblproperties ('orc.compress' = 'snappy');

insert into table hql_first_level.test_three
values (12, '十三', '十三二'),
       (12, null, '十三二'),
       (null, null, '十四三'),
       (null, '十五', '十五四'),
       (15, '十六', '十六五');

select *
from test_three;

select num,
       count(*)             as `*`,
       count(1)             as `1`,
       count(distinct name) as `distinct name`,
       count(name)          as `count name`
from test_three
group by num;

-- 33
select student.stu_id,
       stu_name,
       avg(score) as avg_score
from score
         left join student on score.stu_id = student.stu_id
group by student.stu_id, stu_name
having avg(score) > 85
order by stu_id;

-- 34
select stu_id,
       stu_name,
       collect_set(course_id)   as course_id_set,
       collect_set(course_name) as course_name_set
from (
         select student.stu_id,
                stu_name,
                course.course_id,
                course_name
         from student
                  left join score on student.stu_id = score.stu_id
                  left join course on score.course_id = course.course_id
     ) as table_temp
group by stu_id, stu_name
order by stu_id;

-- 35
select table_temp.course_id,
       course_name,
       sum(`if`(flag = 0, 1, 0)) as `及格人数`,
       sum(`if`(flag = 1, 1, 0)) as `不及格人数`
from (
         select course_id,
                `if`(score < 60, 1, 0) as flag
         from score
     ) as table_temp
         left join course on course.course_id = table_temp.course_id
group by table_temp.course_id, course_name
order by course_id;

-- 36
select score.course_id,
       course_name,
       (sum(`if`(score > 85, 1, 0)))                 as `[100, 85)`,
       (sum(`if`(score > 70 and score <= 85, 1, 0))) as `[85, 70)`,
       (sum(`if`(score > 60 and score <= 70, 1, 0))) as `[70, 60)`,
       (sum(`if`(score <= 60, 1, 0)))                as `[60, 0)`
from score
         left join course on score.course_id = course.course_id
group by score.course_id, course_name
order by score.course_id;

-- 37
select student.stu_id,
       stu_name
from score
         left join student on score.stu_id = student.stu_id
where course_id = '03'
  and score > 80
order by stu_id;

-- 38
select distinct course_id
from score;

select *
from course;

select stu_id,
       sum(`if`(course_id = '01', score, 0)) as `语文`,
       sum(`if`(course_id = '02', score, 0)) as `数学`,
       sum(`if`(course_id = '03', score, 0)) as `英语`,
       sum(`if`(course_id = '04', score, 0)) as `体育`,
       sum(`if`(course_id = '05', score, 0)) as `音乐`
from score
group by stu_id
order by stu_id;

-- 39
select student.stu_id,
       student.stu_name,
       student.birthday,
       student.sex,
       course_id,
       score
from score
         left join student on score.stu_id = student.stu_id
where course_id = '01'
  and score < 60
order by score desc;

-- 40
with table_temp as (
    select stu_name,
           course_name,
           `if`(score > 70, 1, 0) as flag,
           score
    from score
             left join student on score.stu_id = student.stu_id
             left join course on score.course_id = course.course_id
)
select stu_name,
       course_name,
       score
from table_temp
where stu_name in (
    select stu_name
    from table_temp
    group by stu_name
    having sum(flag) > 0
)
  and score > 70
order by stu_name;

-- 41
with table_temp as (
    select stu_name,
           course_name,
           `if`(score < 70, 1, 0) as flag,
           score
    from score
             left join student on score.stu_id = student.stu_id
             left join course on score.course_id = course.course_id
)
select stu_name,
       course_name,
       score
from table_temp
where stu_name not in (
    select stu_name
    from table_temp
    group by stu_name
    having sum(flag) > 0
)
  and score > 70
order by stu_name;

-- 42
select student.stu_id,
       stu_name,
       cast(avg(score) as decimal(4, 2)) as avg_score
from student
         left join score on student.stu_id = score.stu_id
where score < 60
group by student.stu_id, stu_name
having sum(`if`(score < 60, 1, 0)) > 1
order by student.stu_id;

select *
from score
where stu_id = '017';

-- 43
with temp_table as (
    select stu_id,
           score
    from score
    group by stu_id, score
    having count(*) > 1
)
select s.stu_id,
       s.course_id,
       s.score
from score s
         join temp_table t on s.stu_id = t.stu_id and s.score = t.score
order by s.stu_id;

-- 44
select a.stu_id,
       a.course_id,
       a.score
from score a
         join score b on a.stu_id = b.stu_id
where a.score = b.score
  and a.course_id <> b.course_id
order by a.stu_id;

-- 45
select b.stu_id
from score a
         join score b
              on a.stu_id = b.stu_id and a.course_id <> b.course_id
where a.course_id = '02'
  and b.course_id = '01'
  and a.score > b.score
order by stu_id;

select *
from score a
         join score b
              on a.stu_id = b.stu_id and a.course_id <> b.course_id;

select *
from score
where stu_id = '002';

-- 46
select a.stu_id,
       stu_name
from score a
         join score b
              on a.stu_id = b.stu_id and a.course_id <> b.course_id
         left join student s on a.stu_id = s.stu_id
where a.course_id = '02'
  and b.course_id = '01'
order by stu_id;

select *
from score
where course_id in ('01', '02');

-- 47
select a.stu_id,
       stu_name
from score a,
     student t
where a.stu_id = t.stu_id
  and a.course_id = '01'
  and exists (
    select *
    from score b
    where b.course_id = '02'
      and b.stu_id = a.stu_id
)
order by stu_id;

-- 48
with table_temp as (
    select course_id
    from course
    where tea_id in (select tea_id
                     from teacher
                     where tea_name = '李体音')
)
select score.stu_id,
       stu_name
from score
         left join student on score.stu_id = student.stu_id
where course_id in (
    select *
    from table_temp
)
group by score.stu_id, stu_name
having count(*) = (select count(distinct course_id) from table_temp)
order by stu_id;

-- 49
select distinct score.stu_id,
                stu_name
from score
         left join student on score.stu_id = student.stu_id
where course_id in (
    select course_id
    from course
    where tea_id in (select tea_id
                     from teacher
                     where tea_name = '李体音')
)
order by stu_id;

-- 50
select distinct stu_name
from student
         left join score on score.stu_id = student.stu_id
where student.stu_id not in (
    select stu_id
    from (
             select course_id
             from course
             where tea_id in (select tea_id
                              from teacher
                              where tea_name = '李体音')
         ) as table_temp
             join score on score.course_id = table_temp.course_id
)
order by stu_name;

-- 51
select stu_name, course_id, score
from score
         left join student on score.stu_id = student.stu_id
where course_id in (
    select distinct course_id
    from course
    where tea_id in (
        select tea_id
        from teacher
        where tea_name = '李体音'
    )
)
order by score desc
limit 1;

-- 52
select distinct score.stu_id,
                stu_name
from score
         left join student on score.stu_id = student.stu_id
where course_id in (
    select course_id
    from score
    where stu_id = '001'
)
  and score.stu_id <> '001'
order by stu_id;

-- 53
with table_temp as (
    select course_id
    from score
    where stu_id = '001'
)
select student.stu_id, stu_name
from student
         left join score on student.stu_id = score.stu_id
where course_id in (select * from table_temp)
group by student.stu_id, stu_name
having count(distinct course_id) = (select count(*) from table_temp)
   and count(distinct course_id) = (select count(*) from score where score.stu_id = student.stu_id)
   and student.stu_id <> '001'
order by student.stu_id, stu_name;

-- 54
drop table if exists game_user;

create table if not exists game_user
(
    id bigint comment '用户id',
    dt string comment '日期'
) comment ''
    row format delimited fields terminated by '\t';

Insert into game_user
values (1001, '2022-05-01 23:21:33'),
       (1003, '2022-05-02 23:21:33'),
       (1002, '2022-05-01 23:21:33'),
       (1003, '2022-05-01 23:21:33'),
       (1001, '2022-05-03 23:21:33'),
       (1003, '2022-05-04 23:21:33'),
       (1002, '2022-05-01 23:21:33'),
       (1001, '2022-05-05 23:21:33'),
       (1001, '2022-05-01 23:21:33'),
       (1002, '2022-05-06 23:21:33'),
       (1001, '2022-05-06 23:21:33'),
       (1001, '2022-05-07 23:21:33');

select *
from game_user;

select *
from game_user;

select id,
       max(max_day) as max_day
from (
         select id,
                ranking,
                datediff(max(ymd), min(ymd)) + 1 as max_day
         from (
                  select id,
                         ymd,
                         sum(flag) over (partition by id order by ymd asc) as ranking
                  from (
                           select id,
                                  ymd,
                                  `if`(datediff(ymd, lag(ymd, 1, null) over (partition by id order by ymd asc)) <= 2, 0,
                                       1) as flag
                           from (
                                    select id,
                                           date_format(cast(dt as timestamp), 'yyyy-MM-dd') as ymd
                                    from game_user
                                    group by id, date_format(cast(dt as timestamp), 'yyyy-MM-dd')
                                ) as a
                       ) as b
              ) as c
         group by id, ranking
     ) as d
group by id
order by id asc;

set hive.execution.engine = mr;

set hive.execution.engine = spark;

select id,
       date_format(cast(dt as timestamp), 'yyyy-MM-dd') as ymd
from game_user
group by id, date_format(cast(dt as timestamp), 'yyyy-MM-dd')

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

-- 不指定rows between, 默认为从起点到当前行;
--
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

