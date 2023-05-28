-- 查询引擎指定为spark
SET hive.execution.engine=spark;

-- 创建学生表
use firstlevel;

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

load data local inpath '/opt/module/data/score.txt' into table score;

load data local inpath '/opt/module/data/teacher.txt' into table teacher;

load data local inpath '/opt/module/data/course.txt' into table course;

load data local inpath '/opt/module/data/student.txt' into table student;

select count(*) as total
from score;

select count(*) as total
from teacher;

select count(*) as total
from course;

select count(*) as total
from student;

select *
from student
limit 5;

select *
from teacher
limit 5;

select *
from score
limit 5;

-- 1. 查询姓名中带'冰'的学生信息
select *
from student
where stu_name like '%冰%';

-- 2. 查询姓'王'的老师有多少个
select count(*) as teacher_wang_nums
from teacher
where tea_name like '王%';

-- 3. 查询课程编号为04，分数小于60的学生学号，并且按照分数降序排列
select stu_id
from score
where course_id = '05'
  and score < 60
order by score desc;

-- 4. 查询数学成绩不及格的学生的学号、成绩，按照学号升序排列
select s.stu_id,
       s.score
from score s
         left join course c
                   on s.course_id = c.course_id
where c.course_name = '数学'
  and s.score < 60;

-- 5. 查询各学生的年龄，精确到月份
select *
from student;

select stu_name,
       case
           when age_month >= 0 then concat(age_year, '年', age_month, '个月')
           else concat(age_year - 1, '年', 12 + age_month, '个月')
           end as age
from (
         select stu_name,
                year(`current_date`()) - year(birthday)   as age_year,
                month(`current_date`()) - month(birthday) as age_month
         from student
     ) as table_temp;

-- 6. 查询本月过生日的学生
select stu_id,
       stu_name,
       birthday,
       sex
from student
where month(birthday) = month(`current_date`());

-- 7. 查询课程编号为‘02’的总成绩
select *
from course;

select *
from score;

select s.course_id,
       sum(score) as total_score
from score s
         left join course c on s.course_id = c.course_id
where s.course_id = '02'
group by s.course_id;

-- 8. 查询参加考试的学生个数
explain
select count(distinct stu_id) as stu_num_total
from score;

select count(distinct stu_id) as stu_num_total
from score;

set hive.spark.client.server.connect.timeout=900000 ;

-- 9. 查询各科最高分、最低分
select *
from score;

select course_id,
       max(score) as max_score,
       min(score) as min_score
from score
group by course_id
order by course_id asc;

-- 10. 查询每门课程有多少学生参加了考试
select *
from score;

select course_id,
       count(distinct stu_id) as total_stu
from score
group by course_id
order by course_id asc;

-- 11. 查询男生、女生人数
select sex,
       count(stu_id) as total_stu
from student
group by sex;

-- 12. 查询平均成绩大于60分学生的学号和平均成绩
select stu_id,
       avg(score) as avg_score
from score
group by stu_id
having avg_score > 60
order by stu_id asc;

-- 13. 查询至少选修两门课程的学生学号
select stu_id
from score
group by stu_id
having count(distinct course_id) >= 2
order by stu_id asc;

-- 14. 查询同姓学生名单并统计同名人数 -> 注意这里只查找同姓的，所以最后需要筛选人数
with temp_table as (
    select substring(stu_name, 1, 1) as first_name,
       count(*) as stu_total
    from student
    group by substring(stu_name, 1, 1)
)
select first_name,
       s.stu_name,
       stu_total
from student s
left join temp_table t
on substring(s.stu_name, 1, 1) = t.first_name
group by first_name, s.stu_name, stu_total
having stu_total >= 2
order by first_name asc, stu_total asc;

-- 15. 查询每门课程的平均成绩 -> 成绩升序，课程号降序
select
    course_id,
    avg(score) as avg_score
from score
group by course_id
order by avg_score asc, course_id desc;

with temp_table as (
    select
        course_id,
        avg(score) as avg_score
    from score
    group by course_id
)
select course_id,
       round(avg_score, 2) as avg_score
from temp_table
order by avg_score asc, course_id desc;

-- 16. 统计参加考试人数大于等于15的学科
select course_id, count(stu_id) as stu_total
from score
group by course_id
having stu_total >= 15
order by course_id asc;

-- 17. 查询学生的总成绩并进行排名
select
    stu_id,
    sum(score) as total_score
from score
group by stu_id
order by total_score desc, stu_id asc;

-- 18. 查询平均成绩大于60分的学生学号和平均成绩
select
    stu_id,
    avg(score) as avg_score
from score
group by stu_id
having avg_score > 60
order by avg_score desc, stu_id asc;

-- 19. 显示学生的语数外成绩，没成绩的输出为0，按照学生的有效平均成绩降序显示
--     --> 格式: 学生id 学生姓名 语文 数学 英语 有效课程数 有效平均成绩

with temp_table as (
    select
        s1.stu_id,
        stu_name,
        course_id,
        score,
        count(course_id) over(partition by s1.stu_id) as `有效课程数`,
        avg(score) over(partition by s1.stu_id) as `有效平均成绩`
    from score s1
    left join student s2 on s1.stu_id = s2.stu_id
    group by s1.stu_id, course_id, score, stu_name
)
select
    stu_id as `学生id`,
    stu_name as `学生姓名`,
    case when sum(`if`(course_id = '01', score, null)) is not null then sum(if(course_id = '01', score, null)) else 0 end as `语文`,
    case when sum(`if`(course_id = '02', score, null)) is not null then sum(`if`(course_id = '02', score, null)) else 0 end as `数学`,
    case when sum(`if`(course_id = '03', score, null)) is not null then sum(`if`(course_id = '03', score, null)) else 0 end as `英语`,
    max(`有效课程数`) as `有效课程数`,
    max(`有效平均成绩`) as `有效平均成绩`
from temp_table
group by stu_id, stu_name
order by `有效平均成绩` desc, stu_id asc;


-- 20. 查询一共参加两门课程并且其中一门为语文课程的学生id和姓名
select
    sc.stu_id,
    stu_name
from score sc
left join student st on sc.stu_id = st.stu_id
group by sc.stu_id, stu_name
having count(course_id) = 2 and sc.stu_id in (
    select stu_id
    from score
    where course_id = '02'
)
order by sc.stu_id asc;


// 参加三门课程的
select
    sc.stu_id,
    stu_name
from score sc
left join student st on sc.stu_id = st.stu_id
group by sc.stu_id, stu_name
having count(course_id) = 3
order by sc.stu_id asc;


-- 21. 查询所有课程成绩小于60分的学生学号及姓名 -> 注意，周杰伦没有考过试
select
    st.stu_id,
    stu_name
from score sc
right join student st on sc.stu_id = st.stu_id
group by st.stu_id, stu_name
having sum(`if`(score >= 60, 1, 0)) < 1
order by st.stu_id asc;

select *
from student
where stu_name = '周杰伦';

select *
from score
where stu_id = '003';


-- 22. 查询没有学全所有课的学生的学号和姓名
select stu_id, stu_name
from student
where stu_id not in (
    select stu_id
    from score
    group by stu_id
    having count(distinct course_id) = (
        select count(course.course_id)
        from course
    )
);

select *
from course;


-- 23. 查询只选修了两门课程的学生的学号及姓名
select sc.stu_id,
       stu_name
from score sc
left join student st on sc.stu_id = st.stu_id
group by sc.stu_id, stu_name
having count(course_id) = 2
order by stu_id asc;


-- 24. 查找1990年出生的学生
select stu_name
from student
where year(birthday) = 1990
order by stu_name asc;


-- 25. 查询两门以上不及格课程的同学的学号及平均成绩
select st.stu_id,
       avg(score) as avg_score
from student st
left join score sc
on st.stu_id = sc.stu_id
group by st.stu_id
having count(`if`(score < 60 or score is null, 1, null)) >= 2
order by st.stu_id;

select *
from score
where stu_id in ('007', '008', '010', '013', '014', '015', '017', '018', '019', '020')
order by stu_id asc;


select st.stu_id,
       avg(score) as avg_score
from student st
left join score sc
on st.stu_id = sc.stu_id
group by st.stu_id
having count(`if`(score < 60, 1, null)) >= 2
order by st.stu_id;

select null < 60 as test;


-- 26. 查询所有学生的学号、姓名、选课数、总成绩
select
    st.stu_id,
    stu_name,
    count(course_id) as course_nums,
    sum(score) as score_total
from student st
left join score sc on st.stu_id = sc.stu_id
group by st.stu_id, stu_name
order by st.stu_id asc;

-- 27. 查询平均成绩大于85的所有学生的学号、姓名和平均成绩
select
    st.stu_id,
    stu_name,
    avg(score) as avg_score
from student st
left join score sc on st.stu_id = sc.stu_id
group by st.stu_id, stu_name
having avg_score > 85;

-- 28. 查询学生的选课情况：学号、姓名、课程号、课程名称
with temp_table as (
    select sc.stu_id,
           stu_name,
           course_id
    from score sc
    left join student st on sc.stu_id = st.stu_id
)
select
    stu_id,
    stu_name,
    c.course_id,
    course_name
from temp_table t
left join course c
on t.course_id = c.course_id
order by stu_id asc, course_id asc;

-- 29. 查询每门课程的及格人数和不及格人数
with temp_table as (
    select course_id,
           count(`if`(score >= 60, 1, null)) as pass_nums,
           count(`if`(score < 60, 1, null))  as fail_nums
    from score
    group by course_id
)
select
    c.course_id,
    c.course_name,
    pass_nums,
    fail_nums
from temp_table t
left join course c
on t.course_id = c.course_id
order by c.course_id asc;

-- 30. 用分段 -> [100, 85], [85, 70], [70, 60], [< 60]
--     来统计各科成绩，统计各分数段人数，课程号，课程名称
select *
from score;

select
    s.course_id,
    course_name,
    count(`if`(score > 85, 1, null)) as `score[100, 85]`,
    count(`if`(score <= 85 and score > 70, 1, null)) as `score[85, 70]`,
    count(`if`(score <= 70 and score > 60, 1, null)) as `score[70, 60]`,
    count(`if`(score <= 60, 1, null)) as `score[<= 60]`
from score s
left join course c on s.course_id = c.course_id
group by s.course_id, course_name
order by s.course_id asc, course_name asc;


-- 31. 查询课程编号为‘03’并且课程成绩在80分以上的学生的学号和姓名
select
    sc.stu_id,
    stu_name
from score sc
left join student st on sc.stu_id = st.stu_id
where course_id = '03' and score > 80
order by sc.stu_id asc;


-- 32. 行转列 -> 学号 课程01 课程02 课程03 课程04 -> 没有成绩的用0代替
select
    stu_id as `学号`,
    sum(`if`(course_id = '01', score, 0)) as `课程01`,
    sum(`if`(course_id = '02', score, 0)) as `课程02`,
    sum(`if`(course_id = '03', score, 0)) as `课程03`,
    sum(`if`(course_id = '04', score, 0)) as `课程04`
from score
group by stu_id
order by stu_id asc;


-- 33. 检索‘01’课程分数小于60的学生信息，按照分数降序排列
select
    st.stu_id, stu_name, birthday, sex, course_id, score
from student st
left join score sc on st.stu_id = sc.stu_id
where course_id = '01' and score < 60
order by score desc;


-- 34. 查询只有一门课程成绩超70分的学生的姓名、课程名和分数
with temp_table as (
    select stu_name,
           course_name,
           score
    from (
             select sc.stu_id,
                    course_id,
                    score,
                    stu_name,
                    birthday,
                    sex
             from score sc
                      left join student st on st.stu_id = sc.stu_id
         ) as table_temp
             left join course
                       on table_temp.course_id = course.course_id
    where score > 70
)
select
    stu_name,
    max(course_name) as course_name,
    max(score) as course_name
from temp_table
group by stu_name
having count(*) = 1;

-- 方式二
select
    sc.stu_id,
    stu_name,
    course_name,
    score
from score sc
left join student s on sc.stu_id = s.stu_id
left join course c on sc.course_id = c.course_id
where score > 70
order by score asc;

-- 35. 查询所有课程成绩均超70分的学生的姓名、课程名和分数
select
    sc.stu_id,
    stu_name,
    course_name,
    score
from score sc
left join course c on sc.course_id = c.course_id
left join student s on sc.stu_id = s.stu_id
where sc.stu_id not in (
    select stu_id
    from score
    where score <= 70
) and sc.stu_id in (
    select stu_id
    from score
    group by stu_id
    having count(distinct course_id) = 4
)
order by sc.stu_id asc;

-- 36. 查询两门及其以上不及格课程的同学的学号，姓名，以及平均成绩
select
    sc.stu_id,
    stu_name,
    avg(score) as avg_score
from score sc
left join student st on sc.stu_id = st.stu_id
group by sc.stu_id, stu_name
having count(`if`(score < 60, 1, null)) >= 2
order by sc.stu_id asc;

-- 查询的平均成绩是不及格的平均成绩
with temp_table as (
    select sc.stu_id,
           stu_name
    from score sc
             left join student st on sc.stu_id = st.stu_id
    group by sc.stu_id, stu_name
    having count(`if`(score < 60, 1, null)) >= 2
)
select
    temp_table.stu_id,
    stu_name,
    nopass_avg_score
from temp_table
left join (
    select
        stu_id,
        avg(score) as nopass_avg_score
    from score
    where score < 60
    group by stu_id
) as table_temp
on temp_table.stu_id = table_temp.stu_id
order by temp_table.stu_id asc;

select *
from score
where stu_id = '017';

-- 37. 查询不同课程，成绩相同的学生的学生编号、课程编号、学生成绩 -> 某学生的两个及以上科目分数相同，输出其信息
with temp_table as (
    select stu_id,
           score
    from score
    group by stu_id, score
    having count(*) >= 2
)
select
    s.stu_id,
    course_id,
    s.score
from score s
join temp_table t
on s.stu_id = t.stu_id and s.score = t.score
order by stu_id asc, course_id asc;

-- 38. 查询课程编号为‘01’的课程比‘02’的课程成绩高的所有学生的学号
with temp_table as (
    select *
    from score
    where course_id = '01' or course_id = '02'
)
select t1.stu_id
from temp_table t1
group by stu_id
having min(score) <> max(score) and max(score) = (
    select score
    from temp_table t2
    where t2.stu_id = t1.stu_id and course_id = '01'
)
order by t1.stu_id asc;

-- 39. 查询课程编号为‘01’的课程比‘02’的课程成绩低的所有学生的学号
select distinct s1.stu_id
from score s1
left join score s2
on s1.stu_id = s2.stu_id
where s1.course_id = '01' and s2.course_id = '02' and s1.score < s2.score;

select *
from score
where stu_id in ('002', '004', '006', '007', '009', '012', '016', '018') and course_id in ('01', '02')
order by stu_id asc, course_id asc;

-- 40. 查询学过编号为‘01’的课程并且也学过编号为‘02’课程的学生的学号和姓名
with temp_table as (
    select stu_id
    from score
    where course_id = '01'
      and stu_id in (
        select stu_id
        from score
        where course_id = '02'
    )
)
select s.stu_id,
       stu_name
from temp_table t
left join student s
on t.stu_id = s.stu_id
order by stu_id asc;

select *
from score
where stu_id in ('018', '019', '020')
order by stu_id asc;


-- 41. 查询学过‘李体音’老师教过的所有课的同学的学号和姓名
with temp_table as (
    select
        course_id
    from teacher t
    left join course c on t.tea_id = c.tea_id
    where tea_name = '李体音'
)
select
    table_temp.stu_id,
    stu_name
from (
    select
        stu_id,
        course_id
    from score
    where course_id in (select course_id from temp_table)
) as table_temp
left join student
on table_temp.stu_id = student.stu_id
group by table_temp.stu_id, stu_name
having count(distinct course_id) = (select count(course_id) from temp_table)
order by table_temp.stu_id asc;

select *
from teacher;

select *
from course;


-- 42. 查询学过‘李体音’老师讲授的任意一门课程的学生的学号和姓名 -> 注意这里的distinct
with temp_table as (
    select course_id
    from course c
    left join teacher t on c.tea_id = t.tea_id
    where tea_name = '李体音'
)
select
    distinct score.stu_id,
    stu_name
from score
left join student s on score.stu_id = s.stu_id
where course_id in (select * from temp_table)
order by score.stu_id asc;


-- 43. 查询没学过‘李体音’老师讲过的任意一门课的学生和姓名
with temp_table as (
    select course_id
    from course c
    left join teacher t on c.tea_id = t.tea_id
    where tea_name = '李体音'
)
select
    stu_id,
    stu_name
from student
where stu_id not in (
    select
        distinct stu_id
    from score
    where course_id in (select * from temp_table)
)
order by stu_id asc;

-- 44. 查询选修‘李体音’老师所授课程的学生中成绩最高的学生姓名和成绩，用limit 1得出最高一个
with temp_table as (
    select course_id
    from course
             left join teacher
                       on course.tea_id = teacher.tea_id
    where tea_name = '李体音'
)
select stu_name,
       score
from score
left join student s on score.stu_id = s.stu_id
where course_id in (select * from temp_table)
order by score desc
limit 1;


-- 45. 查询至少有一门课与学号为‘001’的学生所学课程相同的学生学号和姓名 -> 注意排除本人
with temp_table as (
    select course_id
    from score
    where stu_id = '001'
)
select
    distinct score.stu_id,
    stu_name
from score
left join student s on score.stu_id = s.stu_id
where course_id in (select * from temp_table) and score.stu_id != '001'
order by score.stu_id asc;

select *
from score
where stu_id = '001';

-- 46. 查询所学课程与学号为‘001’的学生所学课程完全相同的学生的学号和姓名
with temp_table as (
    select course_id,
           count(course_id) over (partition by flag) as course_total
    from (
        select course_id, 1 as flag
        from score
        where stu_id = '001'
    ) as table_temp
)
select score.stu_id,
       stu_name
from score
left join student s on score.stu_id = s.stu_id
group by score.stu_id, stu_name
having score.stu_id not in (
    select score.stu_id
    from score
    where course_id not in (select course_id from temp_table)
) and count(course_id) = (select distinct course_total from temp_table) and score.stu_id != '001'
order by score.stu_id asc;

-- 47. 按照平均成绩从高到低显示所有学生的所有课程成绩以及平均成绩
select stu_id, course_id, score, avg(score) over(partition by stu_id) as avg_score
from score
order by avg_score desc, stu_id asc, course_id asc;

-- 48. 查询每个学生的平均成绩以及名次
with temp_table as (
    select stu_id,
           avg(score) as avg_score,
           1 as flag
    from score
    group by stu_id
)
select stu_id,
       avg_score,
       dense_rank() over (partition by flag order by avg_score desc) as ranking
from temp_table
order by ranking asc, stu_id asc;

-- 49. 按照各科成绩进行排序，并显示在这个学科中的排名
with temp_table as (
    select st.stu_id, stu_name, course_id, score
    from student st
    left join score sc on st.stu_id = sc.stu_id
)
select stu_id,
       stu_name,
       course_id,
       score,
       rank() over (partition by course_id order by score desc) as subject_rank
from temp_table
where course_id is not null
order by course_id desc, subject_rank asc;

-- 50. 查询每门课程成绩最好的前两名学生姓名
with temp_table as (
    select
        stu_id,
        course_id,
        rank() over (partition by course_id order by score desc) as ranking
    from score
)
select course_name,
       stu_name,
       ranking
from temp_table
left join course on temp_table.course_id = course.course_id
left join student on temp_table.stu_id = student.stu_id
where ranking <= 2
order by course_name asc, ranking asc;

select *
from course;


-- 51. 查询所有课程的成绩第2名到第3名的学生信息以及该课程成绩
with temp_table as (
    select
        stu_id,
        course_id,
        score,
        dense_rank() over (partition by course_id order by score desc) as ranking
    from score
)
select course_name,
       score,
       stu_name,
       sex,
       birthday,
       ranking
from temp_table
left join course on temp_table.course_id = course.course_id
left join student on temp_table.stu_id = student.stu_id
where ranking in (2, 3)
order by course_name asc, ranking asc;

-- 52. 查询各科成绩前三名 -> dense_rank
with temp_table as (
    select
        stu_id,
        course_id,
        score,
        dense_rank() over (partition by course_id order by score desc) as ranking
    from score
)
select course_name,
       score,
       stu_name,
       sex,
       birthday,
       ranking
from temp_table
left join course on temp_table.course_id = course.course_id
left join student on temp_table.stu_id = student.stu_id
where ranking in (1, 2, 3)
order by course_name asc, ranking asc;

select `current_timestamp`();
-- 初级题目已完结，用了一天多
-- 2023-05-28 16:03:43.387000000