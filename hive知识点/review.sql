-- hive知识点

-- 1. 模糊搜索表
show tables like '*stu*'

-- stud
-- student

-- 2. 表结构信息
desc student;

-- id,      int,       ""
-- name,    string,    ""

-- 3. 分区信息
show partitions gmall.dim_user_zip;

-- dt=2020-06-14
-- dt=9999-12-31

-- 4. 加载本地文件
load data local inpath '/opt/module/datas/person.json' overwrite into table default.person_info;

-- 5. 通过查询给table插入数据
drop table if exists default.dim_user_zip_tmp;

create table default.dim_user_zip_tmp
(
    id           string comment '编号',
    login_name   string comment '用户名称',
    nick_name    string comment '用户昵称',
    name         string comment '用户姓名',
    phone_num    string comment '手机号码',
    email        string comment '邮箱',
    user_level   string comment '用户级别',
    birthday     string comment '用户生日',
    gender       string comment '性别(M=男, F=女)',
    create_time  string comment '创建时间',
    operate_time string comment '修改时间',
    start_date   string comment '开始日期',
    end_date     string comment '结束日期'
) comment ''
    partitioned by (dt string);

insert overwrite table default.dim_user_zip_tmp partition (dt)
select *
from gmall.dim_user_zip;

select *
from dim_user_zip_tmp;

-- 	Permission	Owner	Group	    Size	Last Modified	Replication	Block Size	Name
-- 	drwxr-xr-x	baiyao	supergroup	0 B	    Jul 11 18:39	0	        0 B	        dt=2020-06-14
-- 	drwxr-xr-x	baiyao	supergroup	0 B	    Jul 11 18:39	0	        0 B	        dt=9999-12-31

-- 6. 导出数据到本地目录
insert overwrite local directory '/opt/module/datas/test_student'
select *
from student
order by id;

-- 7. 原表在hdfs上面存的东西
-- 	Permission	Owner	Group	    Size	Last Modified	Replication	Block Size	Name
-- 	-rw-r--r--	baiyao	supergroup	12 B	Jul 07 00:15	1	        128 MB	    000000_0

-- /opt/module/datas/test_student/000000_0

-- 8. 创建表时指定的一些属性
-- ●  字段分隔符：row format delimited fields terminated by '\t'
-- ●  行分隔符：row format delimited lines terminated by '\n'
-- ●  文件格式为文本型存储：stored as textfile

-- 9. 命令行操作:
-- hive -e 'select * from default.student'

-- [baiyao@zoo1 ~]$ hive -e 'select * from default.student'
-- which: no hbase in (/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/opt/module/jdk/jdk1.8.0_212/bin:/opt/module/hadoop/hadoop-3.1.3/bin:/opt/module/hadoop/hadoop-3.1.3/sbin:/opt/module/zookeeper/zookeeper-3.5.7/bin:/opt/module/kafka/kafka_2.12-3.0.0/bin:/opt/module/kafka_eagle/eagle/bin:/opt/module/hive/apache-hive-3.1.2-bin/bin:/opt/module/spark/spark-3.0.0-bin-hadoop3.2/bin:/home/baiyao/.local/bin:/home/baiyao/bin)
-- Hive Session ID = 7007c569-de69-4b22-9e63-8e2996571bbb
--
-- Logging initialized using configuration in jar:file:/opt/module/hive/apache-hive-3.1.2-bin/lib/hive-common-3.1.2.jar!/hive-log4j2.properties Async: true
-- Hive Session ID = 66f88a2e-fc63-4073-bffe-836f010c7208
-- OK
-- 1	abc
-- 2	def
-- Time taken: 6.286 seconds, Fetched: 2 row(s)

-- 执行查询, 显示进度，执行完毕后，把查询结果输出到终端上，接着hive进程退出。

-- hive -S -e 'select * from default.student'

-- [baiyao@zoo1 ~]$ hive -S -e 'select * from default.student'
-- which: no hbase in (/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/opt/module/jdk/jdk1.8.0_212/bin:/opt/module/hadoop/hadoop-3.1.3/bin:/opt/module/hadoop/hadoop-3.1.3/sbin:/opt/module/zookeeper/zookeeper-3.5.7/bin:/opt/module/kafka/kafka_2.12-3.0.0/bin:/opt/module/kafka_eagle/eagle/bin:/opt/module/hive/apache-hive-3.1.2-bin/bin:/opt/module/spark/spark-3.0.0-bin-hadoop3.2/bin:/home/baiyao/.local/bin:/home/baiyao/bin)
-- Hive Session ID = f7373530-0a7d-42a6-8838-9175ea132fa9
-- Hive Session ID = ddd98ac6-e268-4a9b-a192-bd37e7ce6ef7
-- 1	abc
-- 2	def

-- 不显示进度信息, 直接输出查询结果

-- 10. 修改表名
alter table default.stud
    rename to default.stu;

select *
from stu;

-- zhang3,    bj,    math,       88
-- li4,       bj,    math,       99
-- wang5,     sh,    chinese,    92
-- zhao6,     sh,    chinese,    54
-- tian7,     bj,    chinese,    91

-- 11. 复制表结构
create table default.person like default.person_info;

desc person;

-- name,       string,                                  from deserializer
-- friends,    array<string>,                           from deserializer
-- children,   "map<string,    int>",                   from deserializer
-- address,    "struct<street:string, city:string>",    from deserializer

-- 12. 添加字段
desc stu;

-- name,    string,     ""
-- area,    string,     ""
-- course,  string,     ""
-- score,   int,        ""

// 注意, 这条命令对建表时指定'row format serde'的表失效
alter table stu
    add columns (position string comment '职业名称');

desc stu;

-- name,       string,    ""
-- area,       string,    ""
-- course,     string,    ""
-- score,      int,       ""
-- position,   string,    职业名称

-- 13. 修改字段
alter table stu
    change position position_id string comment '职业id';

desc stu;

-- name,           string,     ""
-- area,           string,     ""
-- course,         string,     ""
-- score,          int,        ""
-- position_id,    string,     职业id

-- 14. 删除分区
-- 	Permission	Owner	Group	    Size	            Last Modified	Replication	Block Size	Name
-- 	drwxr-xr-x	baiyao	supergroup	0 B	                Jul 11 18:39	0	        0 B	        dt=2020-06-14
-- 	drwxr-xr-x	baiyao	supergroup	0 B	                Jul 11 18:39	0	        0 B	        dt=9999-12-31

alter table dim_user_zip_tmp
    drop partition (dt = '2020-06-14');

-- Permission	Owner	Group	    Size	Last Modified	Replication	Block Size	Name
-- drwxr-xr-x	baiyao	supergroup	0 B	    Jul 11 18:39	0	        0 B	        dt=9999-12-31

-- 15. 添加分区
alter table dim_user_zip_tmp
    add partition (dt = '2023-07-14');

-- Permission	Owner	Group	    Size	Last Modified	Replication	Block Size	Name
-- drwxr-xr-x	baiyao	supergroup	0 B	    Jul 11 20:12	0	        0 B	        dt=2023-07-14
-- drwxr-xr-x	baiyao	supergroup	0 B	    Jul 11 18:39	0	        0 B	        dt=9999-12-31

-- 16. 删除空数据库
create database baiyao;

drop database baiyao;

-- 17. 强制删除数据库
create database baiyao;

create table baiyao.baiyao
(
    a string,
    b string
) comment ''
    row format delimited fields terminated by '\t';

insert into table baiyao.baiyao
values ('1', 'a'),
       ('2', 'b');

-- [08S01][1] Error while processing statement: FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask.
-- InvalidOperationException(message:Database baiyao is not empty. One or more tables exist.)
drop database baiyao;

-- 强制删除数据库
drop database baiyao cascade;

-- 18. 删除表
drop table default.person;

-- 19. 清空表
select *
from person_info;

-- songsong,   "[""bingbing"",""lili""]","{""xiao song"":18,""xiaoxiao song"":19}",    "{""street"":""hui long guan"",""city"":""beijing""}"

truncate table person_info;

-- 20. 向hive表中加载数据
-- (1) 直接向分区表插入数据
drop table score;

create table score
(
    a string,
    b string,
    c string
) comment ''
    partitioned by (month string)
    row format delimited fields terminated by ','
    stored as textfile;

insert into table score partition (month = '2023-07-14')
values ('1', '2', '3');

-- Permission	Owner	Group	    Size	Last Modified	Replication	Block Size	Name
-- drwxr-xr-x	baiyao	supergroup	0 B	    Jul 11 20:24	0	        0 B	        month=2023-07-14

select *
from score;

-- 1,   2,  3,  2023-07-14

-- (2) 通过load方式加载数据
load data local inpath '/opt/module/datas/score_textfile.dat' overwrite into table score partition (month = '2023-07-15');

-- Permission	Owner	Group	    Size	Last Modified	Replication	Block Size	Name
-- drwxr-xr-x	baiyao	supergroup	0 B	    Jul 11 20:29	0	        0 B	        month=2023-07-14
-- drwxr-xr-x	baiyao	supergroup	0 B	    Jul 11 20:32	0	        0 B	        month=2023-07-15

select *
from score;

-- 1,    2,    3,    2023-07-14
-- 2,    2,    4,    2023-07-15

-- (3) 通过查询方式加载数据
insert into table score partition (month = '2023-07-16') select a, b, c from score where month = '2023-07-14';

select *
from score;

-- 1,    2,    3,    2023-07-14
-- 2,    2,    4,    2023-07-15
-- 1,    2,    3,    2023-07-16

insert overwrite table score partition (month = '2023-07-17') select a, b, c from score where month = '2023-07-14';

select *
from score;

-- 1,    2,    3,    2023-07-14
-- 2,    2,    4,    2023-07-15
-- 1,    2,    3,    2023-07-16
-- 1,    2,    3,    2023-07-17

-- Permission	Owner	Group	    Size	Last Modified	Replication	Block Size	Name
-- drwxr-xr-x	baiyao	supergroup	0 B	    Jul 11 20:29	0	        0 B	        month=2023-07-14
-- drwxr-xr-x	baiyao	supergroup	0 B	    Jul 11 20:32	0	        0 B	        month=2023-07-15
-- drwxr-xr-x	baiyao	supergroup	0 B	    Jul 11 20:36	0	        0 B	        month=2023-07-16
-- drwxr-xr-x	baiyao	supergroup	0 B	    Jul 11 20:37	0	        0 B	        month=2023-07-17

-- (4) 查询语句中创建表并加载数据
create table score_copy as select * from score;

-- 	Permission	Owner	Group	    Size	Last Modified	Replication	Block Size	    Name
-- 	-rw-r--r--	baiyao	supergroup	68 B	Jul 11 20:40	1	              128 MB	000000_0

select *
from score_copy;

-- 1,    2,    3,    2023-07-14
-- 2,    2,    4,    2023-07-15
-- 1,    2,    3,    2023-07-16
-- 1,    2,    3,    2023-07-17

-- (5) 在创建表时通过location指定加载数据的路径
create external table score_1 (x string, y string, z string) row format delimited fields terminated by ',' location '/user/hive/warehouse/score_source';

select *
from score_1;

-- 1,   2,  3

-- (6) export导出与import导入hive表数据(内部表操作)
export table score_1 to '/export/score';

-- /export/score

-- Permission	Owner	Group	    Size	Last Modified	Replication	Block Size	Name
-- -rw-r--r--	baiyao	supergroup	1.31 KB	Jul 11 21:05	1	        128 MB	    _metadata
-- drwxr-xr-x	baiyao	supergroup	0 B	    Jul 11 21:05	0	        0 B	        data

-- /export/score/data

-- Permission	Owner	Group	    Size	Last Modified	Replication	Block Size	Name
-- -rw-r--r--	baiyao	supergroup	6 B	    Jul 11 21:05	1	        128 MB	    000000_0

drop table if exists score_2;

create table score_2 like score_1;

-- 这条语句执行不了, 未找到问题, 用下面的导入语句了
import table score_2 from '/export/score';

load data inpath '/export/score/data' into table score_2;

select *
from score_2;

-- 1,2,3

-- 21. hive表中数据导出
-- (1)

-- hive函数
-- 1. 聚合函数
-- (1) count(), max(), min(), sum(), avg()
-- count(*) 包含null值，统计所有行数
-- count(id) 不包含null值
-- min 求最小值是不包含null，除非所有值都是null
-- avg 求平均值也是不包含null

create table test (
    a int
) comment ''
row format delimited fields terminated by '\t';

insert into table test values (1), (2), (5), (null);

select *
from test;

-- 1
-- 2
-- 5
-- <null>

select count(*) as `count(*)`,
       count(a) as `count(a)`,
       min(a) as `min(a)`,
       avg(a) as `avg(a)`
from test;

-- 4,    3,    1,    2.6666666666666665

-- (2)
select var_pop(a)
from test;

-- 2.8888888888888893

select var_samp(a)
from test;

-- 4.333333333333334

-- 2. 关系运算
-- (1) =, !=, <>, >, <, >=, <=, is null, is not null

-- (2) A like B:
-- 描述: 如果字符串A或者字符串B为NULL，则返回NULL；
-- 如果字符串A符合表达式B 的正则语法，则为TRUE；否则为FALSE。
-- B中字符”_”表示任意单个字符，而字符”%”表示任意数量的字符。

-- 3. 数学运算
-- (1) +, -, *, /, %, 位与&, 位或|, 位异或^, 位取反~

-- 4. 逻辑运算
-- (1) and, or, not

-- 5. 数值运算
-- (1) round double -> bigint 四舍五入
select round(3.1415926) as a;

-- 3

-- (2) round double -> double 指定精度
select round(3.1415926, 4) as a;

-- 3.1416

-- (3) floor double -> bigint 向下取整
select `floor`(3.89) as a;

-- 3

select `floor`(-3.89) as a;

-- -4

-- (4) ceil double -> bigint 向上取整
select ceil(3.1) as a;

-- 4

-- (5) rand double
-- 返回一个0到1范围内的随机数
select rand();

-- 0.5123843158446442
-- 0.7195105361153438

-- 只要指定种子，每次执行此语句得到的结果一样的
select rand(100);

-- 0.7220096548596434
-- 0.7220096548596434

-- (6) exp double -> 返回自然对数e的n次方
select exp(1) as a, exp(2) as b;

-- 2.718281828459045,7.38905609893065

-- (7) log10() 返回以10为底的n的对数
select log10(100) as a, log10(10) as b, log2(8) as c;

-- 2,   1,   3

-- (8) pow(x, y)返回x的y次幂 double
select pow(1, 3) as a, pow(2, 3) as b;

-- 1,   8

-- (9) sqrt(16) 返回平方根
select sqrt(16) as a, sqrt(4) as b;

-- 4,   2

-- (10) bin(x) 返回x的二进制代码
select bin(2) as a, bin(4) as b;

-- 10,  100

-- 6. 条件函数
-- (1) if (,,)
select `if`(1=1, 'true', 'false');

-- true

select `if`(1=2, 'true', 'false');

-- false

-- (2) coalesce() : 返回参数中的第一个非空值；
--                  如果所有值都为NULL，那么返回NULL
select `coalesce`(null, 'true', '1', '5');

-- true

select `coalesce`(null, 2, 3, 4);

-- 2

-- (3) case when 两种写法
select case when 1=2 then 'tom' when 2=2 then 'mary' else 'tim' end;

-- mary

Select case 100 when 50 then 'tom' when 100 then 'mary' else 'tim' end;

-- mary

-- 7. 日期函数
-- (1) unix_timestamp() 获得当前时区的UNIX时间戳
select unix_timestamp() as ts;

-- 1689085045

-- (2) from_unixtime, 转化UNIX时间戳（从1970-01-01 00:00:00 UTC到指定时间的秒数）到当前时区的时间格式。
select from_unixtime(1689085045, 'yyyy-MM-dd HH:mm:ss') as ts;

-- 2023-07-11 14:17:25

-- (3) unix_timestamp, 转换格式为"yyyy-MM-dd HH:mm:ss"的日期到UNIX时间戳。如果转化失败，则返回0。
select unix_timestamp('2023-07-11 14:17:25') as a;

-- 1689085045

-- (4) unix_timestamp, 转换pattern格式的日期到UNIX时间戳。如果转化失败，则返回0。
select unix_timestamp('2023-07-11 14:17:25', 'yyyy-MM-dd HH:mm:ss') as a;

-- 1689085045

-- (5) to_date  返回日期
select to_date('2023-07-11 14:17:25') as a;

-- 2023-07-11

-- (6) year, month, day, hour, minute, second, weekofyear
select year('2023-07-11 14:17:25') as year,
       month('2023-07-11 14:17:25') as month,
       day('2023-07-11 14:17:25') as day,
       hour('2023-07-11 14:17:25') as hour,
       minute('2023-07-11 14:17:25') as minute,
       second('2023-07-11 14:17:25') as second,
       weekofyear('2023-07-11 14:17:25') as weekofyear;

-- 2023,7,11,14,17,25,28

-- (7) datediff, date_add, date_sub
select datediff('2020-12-08','2020-05-09') as datediff,
       date_add('2020-12-08',10) as dateadd,
       date_sub('2020-12-08',10) as datesub;

-- 213, 2020-12-18, 2020-11-28

--