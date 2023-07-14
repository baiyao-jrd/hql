-- hive知识点

-- 1. 模糊搜索表
show tables like '*stu*';

-- stud
-- student

-- 2. 表结构信息
desc student;

desc database default;

-- default,     Default Hive database,      hdfs://zoo1:8020/user/hive/warehouse,       public,     ROLE,        ""

-- 查看数据库更多详细信息
desc database extended default;

-- default,     Default Hive database,      hdfs://zoo1:8020/user/hive/warehouse,       public,     ROLE,       ""

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
insert into table score partition (month = '2023-07-16')
select a, b, c
from score
where month = '2023-07-14';

select *
from score;

-- 1,    2,    3,    2023-07-14
-- 2,    2,    4,    2023-07-15
-- 1,    2,    3,    2023-07-16

insert overwrite table score partition (month = '2023-07-17')
select a, b, c
from score
where month = '2023-07-14';

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
create table score_copy as
select *
from score;

-- 	Permission	Owner	Group	    Size	Last Modified	Replication	Block Size	    Name
-- 	-rw-r--r--	baiyao	supergroup	68 B	Jul 11 20:40	1	              128 MB	000000_0

select *
from score_copy;

-- 1,    2,    3,    2023-07-14
-- 2,    2,    4,    2023-07-15
-- 1,    2,    3,    2023-07-16
-- 1,    2,    3,    2023-07-17

-- (5) 在创建表时通过location指定加载数据的路径
create external table score_1
(
    x string,
    y string,
    z string
) row format delimited fields terminated by ',' location '/user/hive/warehouse/score_source';

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
-- (1) insert导出
-- 将查询的结果导出到本地
insert overwrite local directory '/export/servers/exporthive'
select *
from score;

-- 将查询的结果格式化导出到本地：
insert overwrite local directory '/export/servers/exporthive' row format delimited fields terminated by '\t' collection items terminated by '#'
select *
from student;

-- 将查询的结果导出到HDFS上(没有local)：
insert overwrite directory '/export/servers/exporthive' row format delimited fields terminated by '\t' collection items terminated by '#'
select *
from score;

-- (2) hadoop命令导出
-- hadoop fs -get /warehouse/test/t10/000000_0 /opt/module/datas/local.txt;

-- (3) hive shell 命令导出
-- 基本语法：（hive -f/-e 执行语句或者脚本 > file）
-- hive -e "select * from myhive.score;" > /export/servers/exporthive/score.txt
--
-- hive -f export.sh > /export/servers/exporthive/score.txt


-- (4) export导出
export table score to '/export/exporthive/score';

-- **********************************************************************************************************************************************************


-- Hive函数
--
-- 1. 聚合函数
--
-- 1.  指定列值的数目：count()
-- 2.  指定列值求和：sum()
-- 3.  指定列的最大值：max()
-- 4.  指定列的最小值：min()
-- 5.  指定列的平均值：avg()
-- 6.  非空集合总体变量函数：var_pop(col)
-- 7.  非空集合样本变量函数：var_samp (col)
-- 8.  总体标准偏离函数：stddev_pop(col)
-- 9.  分位数函数：percentile(BIGINT col, p)
-- 10.  中位数函数：percentile(BIGINT col, 0.5)
--
-- 2. 关系运算
--
-- 1.  A LIKE B： LIKE比较，如果字符串A符合表达式B 的正则语法，则为TRUE
-- 2.  A RLIKE B：JAVA的LIKE操作，如果字符串A符合JAVA正则表达式B的正则语法，则为TRUE
-- 3.  A REGEXP B：功能与RLIKE相同
--
-- 3. 数学运算
--
-- 支持所有数值类型：加(+)、减(-)、乘(*)、除(/)、取余(%)、位与(&)、位或(|)、位异或(^)、位取反(~)
--
-- 4. 逻辑运算
--
-- 支持：逻辑与(and)、逻辑或(or)、逻辑非(not)
--
-- 5. 数值运算
--
-- 1.  取整函数：round(double a)
-- 2.  指定精度取整函数：round(double a, int d)
-- 3.  向下取整函数：floor(double a)
-- 4.  向上取整函数：ceil(double a)
-- 5.  取随机数函数：rand(),rand(int seed)
-- 6.  自然指数函数：exp(double a)
-- 7.  以10为底对数函数：log10(double a)
-- 8.  以2为底对数函数：log2()
-- 9.  对数函数：log()
-- 10.  幂运算函数：pow(double a, double p)
-- 11.  开平方函数：sqrt(double a)
-- 12.  二进制函数：bin(BIGINT a)
-- 13.  十六进制函数：hex()
-- 14.  绝对值函数：abs()
-- 15.  正取余函数：pmod()
--
-- 6. 条件函数
--
-- 1.  if
-- 2.  case when
-- 3.  coalesce(c1,c2,c3)
-- 4.  nvl(c1，c2)
--
-- 7. 日期函数
--
-- 1.  获得当前时区的UNIX时间戳: unix_timestamp()
-- 2.  时间戳转日期函数：from_unixtime()
-- 3.  日期转时间戳：unix_timestamp(string date)
-- 4.  日期时间转日期函数：to_date(string timestamp)
-- 5.  日期转年函数：year(string date)
-- 6.  日期转月函数：month (string date)
-- 7.  日期转天函数: day (string date)
-- 8.  日期转小时函数: hour (string date)
-- 9.  日期转分钟函数：minute (string date)
-- 10.  日期转秒函数: second (string date)
-- 11.  日期转周函数: weekofyear (string date)
-- 12.  日期比较函数: datediff(string enddate, string startdate)
-- 13.  日期增加函数: date_add(string startdate, int days)
-- 14.  日期减少函数：date_sub (string startdate, int days)
--
-- 8. 字符串函数
--
-- 1.  字符串长度函数：length(string A)
-- 2.  字符串反转函数：reverse(string A)
-- 3.  字符串连接函数: concat(string A, string B…)
-- 4.  带分隔符字符串连接函数：concat_ws(string SEP, string A, string B…)
-- 5.  字符串截取函数: substr(string A, int start, int len)
-- 6.  字符串转大写函数: upper(string A)
-- 7.  字符串转小写函数：lower(string A)
-- 8.  去空格函数：trim(string A)
-- 9.  左边去空格函数：ltrim(string A)
-- 10.  右边去空格函数：rtrim(string A)
-- 11.  正则表达式替换函数： regexp_replace(string A, string B, string C)
-- 12.  正则表达式解析函数: regexp_extract(string subject, string pattern, int index)
-- 13.  URL解析函数：parse_url(string urlString, string partToExtract [, string keyToExtract])
-- 返回值: string
-- 14.  json解析函数：get_json_object(string json_string, string path)
-- 15.  空格字符串函数：space(int n)
-- 16.  重复字符串函数：repeat(string str, int n)
-- 17.  首字符ascii函数：ascii(string str)
-- 18.  左补足函数：lpad(string str, int len, string pad)
-- 19.  右补足函数：rpad(string str, int len, string pad)
-- 20.  分割字符串函数: split(string str, string pat)
-- 21.  集合查找函数: find_in_set(string str, string strList)
--
-- 9. 窗口函数
--
-- 1.  分组求和函数：sum(pv) over(partition by cookieid order by createtime) 有坑，加不加 order by 差别很大，具体详情在下面第二部分。
-- 2.  分组内排序，从1开始顺序排：ROW_NUMBER() 如：1234567
-- 3.  分组内排序，排名相等会在名次中留下空位：RANK() 如：1233567
-- 4.  分组内排序，排名相等不会在名次中留下空位：DENSE_RANK() 如：1233456
-- 5.  有序的数据集合平均分配到指定的数量（num）个桶中：NTILE()
-- 6.  统计窗口内往上第n行值：LAG(col,n,DEFAULT)
-- 7.  统计窗口内往下第n行值：LEAD(col,n,DEFAULT)
-- 8.  分组内排序后，截止到当前行，第一个值：FIRST_VALUE(col)
-- 9.  分组内排序后，截止到当前行，最后一个值: LAST_VALUE(col)
-- 10.  小于等于当前值的行数/分组内总行数：CUME_DIST()
--
-- 以下函数建议看第二部分详细理解下，此处仅简写，！
--
-- 11.  将多个group by 逻辑写在一个sql语句中: GROUPING SETS
-- 12.  根据GROUP BY的维度的所有组合进行聚合：CUBE
-- 13.  CUBE的子集，以最左侧的维度为主，从该维度进行层级聚合：ROLLUP


-- **********************************************************************************************************************************************************


-- 说明：hive的表存放位置模式是由hive-site.xml当中的一个属性指定的 :hive.metastore.warehouse.dir
create database if not exists myhive;

-- 创建数据库并指定hdfs存储位置 :
create database myhive2 location '/myhive2';

-- 可以用alter  database 命令来修改数据库的一些属性。但是数据库的元数据信息是不可更改的，包括数据库的名称以及数据库所在的位置
alter database hive_explode set dbproperties ('createtime' = '20210329');

use myhive; -- 使用myhive数据库

create table stu
(
    id   int,
    name string
);

insert into stu
values (1, "zhangsan");

insert into stu
values (1, "zhangsan"),
       (2, "lisi"); -- 一次插入多条数据

select *
from stu;

-- hive建表时候的字段类型:
-- 分类	    类型	        描述	                                        字面量示例
-- 原始类型	BOOLEAN	    true/false	                                TRUE
-- 	        TINYINT	    1字节的有符号整-128~127	                    1Y
-- 	        SMALLINT	2个字节的有符号整数，-32768~32767	            1S
-- 	        INT	        4个字节的带符号整数	                        1
-- 	        BIGINT	    8字节带符号整数	                            1L
-- 	        FLOAT	    4字节单精度浮点数                              1.0
-- 	        DOUBLE	    8字节双精度浮点数	                            1.0
-- 	        DEICIMAL	任意精度的带符号小数	                        1.0
-- 	        STRING	    字符串，变长	                                “a”,’b’
-- 	        VARCHAR	    变长字符串	                                “a”,’b’
-- 	        CHAR	    固定长度字符串	                                “a”,’b’
-- 	        BINARY	    字节数组	                                    无法表示
-- 	        TIMESTAMP	时间戳，毫秒值精度	                            122327493795
-- 	        DATE	    日期	                                        ‘2016-03-29’
-- 	        INTERVAL	时间频率间隔
-- 复杂类型	ARRAY	    有序的的同类型的集合	                        array(1,2)
-- 	        MAP	        key-value,key必须为原始类型，value可以任意类型	map(‘a’,1,’b’,2)
-- 	        STRUCT	    字段集合,类型可以不同	                        struct(‘1’,1,1.0), named_stract(‘col1’,’1’,’col2’,1,’clo3’,1.0)
--  	    UNION	    在有限取值范围内的一个值	                    create_union(1,’a’,63)

-- 对decimal类型简单解释下：
-- 用法：decimal(11,2) 代表最多有11位数字，其中后2位是小数，整数部分是9位；如果整数部分超过9位，则这个字段就会变成null；如果小数部分不足2位，则后面用0补齐两位，如果小数部分超过两位，则超出部分四舍五入
-- 也可直接写 decimal，后面不指定位数，默认是 decimal(10,0)  整数10位，没有小数

-- 创建表并指定字段之间的分隔符
create table if not exists stu2
(
    id   int,
    name string
) row format delimited fields terminated by '\t'
    stored as textfile
    location '/user/stu2';

-- row format delimited fields terminated by '\t'  指定字段分隔符，默认分隔符为 '\001'
-- stored as 指定存储格式
-- location 指定存储位置

-- 根据查询结果创建表
create table stu3 as
select *
from stu2;

-- 根据已经存在的表结构创建表
create table stu4 like stu2;

-- 查询表的结构
-- 只查询表内字段及属性
desc t3;

-- name,        string,                 ""
-- children,    array<string>,          ""
-- address,     "map<string,string>",   ""

-- 详细查询
desc formatted t3;

-- # col_name,data_type,comment
-- name,string,""
-- children,array<string>,""
-- address,"map<string,string>",""
-- "",,
-- # Detailed Table Information,,
-- Database:           ,hive_explode        ,
-- OwnerType:          ,USER                ,
-- Owner:              ,baiyao              ,
-- CreateTime:         ,Wed Jul 12 05:51:53 GMT+08:00 2023,
-- LastAccessTime:     ,UNKNOWN             ,
-- Retention:          ,0                   ,
-- Location:           ,hdfs://zoo1:8020/user/hive/warehouse/hive_explode.db/t3,
-- Table Type:         ,MANAGED_TABLE       ,
-- Table Parameters:,,
-- "",bucketing_version   ,2
-- "",numFiles            ,1
-- "",numRows             ,0
-- "",rawDataSize         ,0
-- "",totalSize           ,94
-- "",transient_lastDdlTime,1689112317
-- "",,
-- # Storage Information,,
-- SerDe Library:      ,org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe,
-- InputFormat:        ,org.apache.hadoop.mapred.TextInputFormat,
-- OutputFormat:       ,org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat,
-- Compressed:         ,No                  ,
-- Num Buckets:        ,-1                  ,
-- Bucket Columns:     ,[]                  ,
-- Sort Columns:       ,[]                  ,
-- Storage Desc Params:,,
-- "",collection.delim    ,",                   "
-- "",field.delim         ,\t
-- "",mapkey.delim        ,:
-- "",serialization.format,\t

-- 查询创建表的语句
show create table t3;

-- CREATE TABLE `t3`(
-- "  `name` string, "
-- "  `children` array<string>, "
-- "  `address` map<string,string>)"
-- ROW FORMAT SERDE
--   'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
-- WITH SERDEPROPERTIES (
-- "  'collection.delim'=',', "
-- "  'field.delim'='\t', "
-- "  'mapkey.delim'=':', "
--   'serialization.format'='\t')
-- STORED AS INPUTFORMAT
--   'org.apache.hadoop.mapred.TextInputFormat'
-- OUTPUTFORMAT
--   'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
-- LOCATION
--   'hdfs://zoo1:8020/user/hive/warehouse/hive_explode.db/t3'
-- TBLPROPERTIES (
-- "  'bucketing_version'='2', "
--   'transient_lastDdlTime'='1689112317')

-- 对外部表操作
-- 外部表因为是指定其他的hdfs路径的数据加载到表当中来，所以hive表会认为自己不完全独占这份数据，所以删除hive表的时候，数据仍然存放在hdfs当中，不会删掉，只会删除表的元数据
create external table student
(
    s_id   string,
    s_name string
) row format delimited fields terminated by '\t';

-- 从本地文件系统向表中加载数据
-- 追加操作
load data local inpath '/export/servers/hivedatas/student.csv' into table student;

-- 覆盖操作
load data local inpath '/export/servers/hivedatas/student.csv' overwrite into table student;

-- 从hdfs文件系统向表中加载数据
load data inpath '/hivedatas/techer.csv' into table techer;

-- 加载数据到指定分区
load data inpath '/hivedatas/techer.csv' into table techer partition (cur_date = 20201210);

-- 注意：
-- 1.使用 load data local 表示从本地文件系统加载，文件会拷贝到hdfs上
-- 2.使用 load data 表示从hdfs文件系统加载，文件会直接移动到hive相关目录下，注意不是拷贝过去，因为hive认为hdfs文件已经有3副本了，没必要再次拷贝了
-- 3.如果表是分区表，load 时不指定分区会报错
-- 4.如果加载相同文件名的文件，会被自动重命名

-- 对分区表的操作
-- 创建分区表的语法
create table score
(
    s_id    string,
    s_score int
) partitioned by (month string);

-- 创建一个表带多个分区
create table score2
(
    s_id    string,
    s_score int
) partitioned by (year string,month string,day string);

-- 注意：
-- hive表创建的时候可以用 location 指定一个文件或者文件夹，当指定文件夹时，hive会加载文件夹下的所有文件，当表中无分区时，这个文件夹下不能再有文件夹，否则报错
-- 当表是分区表时，比如 partitioned by (day string)， 则这个文件夹下的每一个文件夹就是一个分区，且文件夹名为 day=20201123
-- 这种格式，然后使用：msck  repair   table  score; 修复表结构，成功之后即可看到数据已经全部加载到表当中去了

-- 加载数据到一个分区的表中
load data local inpath '/export/servers/hivedatas/score.csv' into table score partition (month = '201806');

-- 加载数据到一个多分区的表中去
create table test_multi_partition
(
    a string,
    b string
) comment ''
    partitioned by (year string, month string, day string);

insert into table test_multi_partition
values ('1', '2', '2018', '06', '01');

select *
from test_multi_partition;

-- 1,2,2018,06,01

-- /user/hive/warehouse/hive_explode.db/test_multi_partition/year=2018/month=06/day=01
-- Permission	Owner	Group	    Size	Last Modified	Replication	Block Size	Name
-- -rw-r--r--	baiyao	supergroup	4 B	    Jul 12 09:40	1	        128 MB	    000000_0

load data local inpath '/export/servers/hivedatas/score.csv' into table score2 partition (year = '2018',month = '06',day = '01');

-- 查看分区
show partitions score;

-- 同时添加多个分区
-- 注意：添加分区之后就可以在hdfs文件系统当中看到表下面多了一个文件夹
alter table score
    add partition (month = '201804') partition (month = '201803');

-- 对分桶表操作
-- 将数据按照指定的字段进行分成多个桶中去，就是按照分桶字段进行哈希划分到多个文件当中去
-- 分区就是分文件夹，分桶就是分文件
--
-- 分桶优点：
-- 1. 提高join查询效率
-- 2. 提高抽样效率

-- 开启hive的捅表功能
set hive.enforce.bucketing=true;

-- 设置reduce的个数
set mapreduce.job.reduces=3;

-- 创建桶表
create table course
(
    c_id   string,
    c_name string
) clustered by (c_id) into 3 buckets;

-- 桶表的数据加载：由于桶表的数据加载通过hdfs  dfs  -put文件或者通过load  data均不可以，只能通过insert  overwrite 进行加载
-- 所以把文件加载到桶表中，需要先创建普通表，并通过insert  overwrite的方式将普通表的数据通过查询的方式加载到桶表当中去

-- 通过insert  overwrite给桶表中加载数据
insert overwrite table course
select *
from course_common cluster by (c_id);
-- 最后指定桶字段

-- 将course_common表格中的数据按照c_id列的值进行分桶，并将其覆盖到course表格中，以此创建一个分桶表格。
-- course表格是分桶表格，而course_common表格不一定是分桶表格。
--
-- 当使用"INSERT OVERWRITE"语句将查询结果覆盖到course表格中时，查询结果将会被按照指定的列进行分桶，并将每个桶中的数据存储到不同的文件中。
-- 因此，如果在创建course表格时使用了"CLUSTERED BY"和"INTO BUCKETS"关键字，那么course表格就是分桶表格。

-- 可以说ORDER BY是逻辑排序，而CLUSTER BY更关注物理布局的优化。但是，这并不意味着CLUSTER BY会对数据进行物理排序，它只是确保了具有相同键的行会存储在一起。

-- 说明：只能清空管理表，也就是内部表；清空外部表，会产生错误
truncate table score6;

-- **注意：truncate 和 drop：
-- 如果 hdfs 开启了回收站，drop 删除的表数据是可以从回收站恢复的，表结构恢复不了，需要自己重新创建；truncate 清空的表是不进回收站的，所以无法恢复truncate清空的表
-- 所以 truncate 一定慎用，一旦清空将无力回天**

-- **********************************************************************************************************************************************************

-- hive的DQL查询语法: "Data Query Language"
-- (1) 单表查询
-- SELECT [ALL | DISTINCT] select_expr, select_expr, ...
-- FROM table_reference
-- [WHERE where_condition]
-- [GROUP BY col_list [HAVING condition]]
-- [CLUSTER BY col_list
--   | [DISTRIBUTE BY col_list] [SORT BY| ORDER BY col_list]
-- ]
-- [LIMIT number]

-- 注意：
-- 1、order by 会对输入做全局排序，因此只有一个reducer，会导致当输入规模较大时，需要较长的计算时间。
-- 2、sort by不是全局排序，其在数据进入reducer前完成排序。因此，如果用sort by进行排序，并且设置mapred.reduce.tasks>1，则sort by只保证每个reducer的输出有序，不保证全局有序。
-- 3、distribute by(字段)根据指定的字段将数据分到不同的reducer，且分发算法是hash散列。
-- 4、Cluster by(字段) 除了具有Distribute by的功能外，还会对该字段进行排序。
-- 因此，如果分桶和sort字段是同一个时，此时，cluster by = distribute by + sort by

-- where
create table score
(
    a int
) comment '';

insert into table score
values (1),
       (2),
       (null),
       (78),
       (90);

select *
from score;

-- 1
-- 2
-- <null>
-- 78
-- 90

select *
from score
where a < 60;

-- 1
-- 2

-- 小于某个值是不包含null的，如上查询结果是把 s_score 为 null 的行剔除的
create table score_new
(
    a int,
    b int
) comment '';

insert into table score_new
values (1, 1),
       (2, 2),
       (1, 2),
       (78, 1),
       (78, 2);

insert into table score_new
values (95, 1),
       (90, 2);

select *
from score_new;

-- 1,   1
-- 2,   2
-- 1,   2
-- 78,  1
-- 78,  2

-- group by
select a, avg(b)
from score_new
group by a;

-- 78,  1.5
-- 1,   1.5
-- 2,   2

-- 分组后对数据进行筛选，使用having
select *
from score_new;

-- 1,   1
-- 2,   2
-- 1,   2
-- 78,  1
-- 78,  2
-- 95,  1
-- 90,  2

select a, avg(b) avgscore
from score_new
group by a
having a > 85;

-- 95,  1
-- 90,  2

-- 注意：
-- 如果使用 group by 分组，则 select 后面只能写分组的字段或者聚合函数
-- where和having区别：
-- 1 having是在 group by 分完组之后再对数据进行筛选，所以having 要筛选的字段只能是分组字段或者聚合函数
-- 2 where 是从数据表中的字段直接进行的筛选的，所以不能跟在gruop by后面，也不能使用聚合函数

-- join连接
-- INNER JOIN 内连接：只有进行连接的两个表中都存在与连接条件相匹配的数据才会被保留下来
select *
from techer t
         inner join course c
                    on t.t_id = c.t_id;
-- inner 可省略

-- LEFT OUTER JOIN 左外连接：左边所有数据会被返回，右边符合条件的被返回
select *
from techer t
         left join course c on t.t_id = c.t_id;
-- outer可省略

-- RIGHT OUTER JOIN 右外连接：右边所有数据会被返回，左边符合条件的被返回
select *
from techer t
         right join course c on t.t_id = c.t_id;

-- FULL OUTER JOIN 满外(全外)连接: 将会返回所有表中符合条件的所有记录。如果任一表的指定字段没有符合条件的值的话，那么就使用NULL值替代。
SELECT *
FROM techer t
         FULL JOIN course c ON t.t_id = c.t_id;

-- 注：1. hive2版本已经支持不等值连接，就是 join on条件后面可以使用大于小于符号了;并且也支持 join on 条件后跟or (早前版本 on 后只支持 = 和 and，不支持 > < 和 or)
--    2. 如hive执行引擎使用MapReduce，一个join就会启动一个job，一条sql语句中如有多个join，则会启动多个job
--
-- 注意：表之间用逗号(,)连接和 inner join 是一样的
select *
from table_a,
     table_b
where table_a.id = table_b.id;

-- 它们的执行效率没有区别，只是书写方式不同，用逗号是sql 89标准，join 是sql 92标准。用逗号连接后面过滤条件用 where ，用 join 连接后面过滤条件是 on。

-- order by 排序
-- 全局排序，只会有一个reduce
-- ASC（ascend）: 升序（默认） DESC（descend）: 降序
-- 注意：order by 是全局排序，所以最后只有一个reduce，也就是在一个节点执行，如果数据量太大，就会耗费较长时间
SELECT *
FROM student s
         LEFT JOIN score sco ON s.s_id = sco.s_id
ORDER BY sco.s_score DESC;

-- sort by 局部排序
-- 每个MapReduce内部进行排序，对全局结果集来说不是排序。
--
-- 设置reduce个数
set mapreduce.job.reduces=3;
--
-- 查看设置reduce个数
set mapreduce.job.reduces;

-- mapreduce.job.reduces=-1

--
-- 查询成绩按照成绩降序排列
select *
from score sort by s_score;
--
-- 将查询结果导入到文件中（按照成绩降序排列）
insert overwrite local directory '/export/servers/hivedatas/sort'
select *
from score sort by s_score;

-- distribute by  分区排序
-- 注意：Hive要求 distribute by 语句要写在 sort by 语句之前
-- distribute by：类似MR中partition，进行分区，结合sort by使用

-- 设置reduce的个数，将我们对应的s_id划分到对应的reduce当中去
set mapreduce.job.reduces=7;

-- 通过distribute by  进行数据的分区
select *
from score distribute by s_id sort by s_score;

-- cluster by
-- 当distribute by和sort by字段相同时，可以使用cluster by方式.
-- cluster by除了具有distribute by的功能外还兼具sort by的功能。但是排序只能是正序排序，不能指定排序规则为ASC或者DESC。
--
-- 以下两种写法等价
select *
from score cluster by s_id;

select *
from score distribute by s_id sort by s_id;


-- hive函数
-- 1. 聚合函数
-- (1) count(), max(), min(), sum(), avg()
-- count(*) 包含null值，统计所有行数
-- count(id) 不包含null值
-- min 求最小值是不包含null，除非所有值都是null
-- avg 求平均值也是不包含null

create table test
(
    a int
) comment ''
    row format delimited fields terminated by '\t';

insert into table test
values (1),
       (2),
       (5),
       (null);

select *
from test;

-- 1
-- 2
-- 5
-- <null>

select count(*) as `count(*)`,
       count(a) as `count(a)`,
       min(a)   as `min(a)`,
       avg(a)   as `avg(a)`
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
select `if`(1 = 1, 'true', 'false');

-- true

select `if`(1 = 2, 'true', 'false');

-- false

-- (2) coalesce() : 返回参数中的第一个非空值；
--                  如果所有值都为NULL，那么返回NULL
select `coalesce`(null, 'true', '1', '5');

-- true

select `coalesce`(null, 2, 3, 4);

-- 2

-- (3) case when 两种写法
select case when 1 = 2 then 'tom' when 2 = 2 then 'mary' else 'tim' end;

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
select year('2023-07-11 14:17:25')       as year,
       month('2023-07-11 14:17:25')      as month,
       day('2023-07-11 14:17:25')        as day,
       hour('2023-07-11 14:17:25')       as hour,
       minute('2023-07-11 14:17:25')     as minute,
       second('2023-07-11 14:17:25')     as second,
       weekofyear('2023-07-11 14:17:25') as weekofyear;

-- 2023,7,11,14,17,25,28

-- (7) datediff, date_add, date_sub
select datediff('2020-12-08', '2020-05-09') as datediff,
       date_add('2020-12-08', 10)           as dateadd,
       date_sub('2020-12-08', 10)           as datesub;

-- 213, 2020-12-18, 2020-11-28

-- 8. 字符串函数
-- (1) length(字符串长度), reverse(字符串反转), concat(字符串连接), concat_ws(带分隔符字符串连接), substr(字符串截取), substring(字符串截取), upper(转大写), ucase(转大写),
--     lower(转小写), lcase(转小写), trim(去除两边空格), ltrim(去除左边空格), rtrim(去除右边空格), regexp_replace(将字符串A中的符合正则表达式B的部分替换为C),
--     regexp_extract(将字符串subject按照pattern正则表达式的规则拆分，返回index指定的字符。注意，在有些情况下要使用转义字符，下面的等号要用双竖线转义，这是java正则表达式的规则。),
--     parse_url(返回URL中指定的部分。partToExtract的有效值为：HOST, PATH, QUERY, REF, PROTOCOL, AUTHORITY, FILE, and USERINFO.),
--     get_json_object(解析json的字符串json_string,返回path指定的内容。如果输入的json字符串无效，那么返回NULL。), space(返回长度为n的字符串), repeat(返回重复n次后的字符串),
--     ascii(返回字符串str第一个字符的ascii码), lpad(将str进行用pad进行左补足到len位),
--     rpad(将str进行用pad进行右补足到len位), split(按照pat字符串分割str，会返回分割后的字符串数组), find_in_set(返回str在strlist第一次出现的位置，strlist是用逗号分割的字符串。如果没有找该str字符，则返回0)

select length('abcdefg')                     as length,         //7
       reverse('abcdefg')                    as reverse,        //gfedcba
       concat('abc', 'de', 'fgh')            as concat,         //abcdefgh
       concat_ws(',', 'abc', 'de', 'fgh')    as concat_ws,      //abc,de,fgh
       substr('abcdefg', 3, 2)               as substr,         //cd
       substring('abcdefg', 3, 2)            as substring_1,    //cd
       substring('abcdefg', 3, 2)            as substring_2,    //cd
       substring('abcdefg', -3)              as substring_3,    //efg
       upper('abcdefg')                      as upper,          //ABCDEFG
       ucase('abcDefg')                      as ucase,          //ABCDEFG
       lower('ABCDEFG')                      as lower,          //abcdefg
       lcase('ABCDefg')                      as lcase,          //abcdefg
       trim('  a bcd e  ')                   as trim,           //a bcd e
       ltrim('  a bcd e  ')                  as ltrim,          //a bcd e
       rtrim('  a bcd e  ')                  as rtrim,          //  a bcd e
       regexp_replace('foobar', 'oo|ar', '') as regexp_replace, //fb
       space(10)                             as space_10,       //
       length(space(10))                     as space_length,   //10
       repeat('abc ', 5)                     as repeat,         //abc abc abc abc abc
       ascii('abcde')                        as ascii,          //97
       lpad('abc', 10, 'td')                 as lpad,           //tdtdtdtabc
       rpad('abc', 10, 'td')                 as rpad,           //abctdtdtdt
       split('abtcdtef', 't')                as split,          //["ab","cd","ef"]
       find_in_set('ab', 'ef,ab,de')         as find_in_set_1,  //2
       find_in_set('ab', 'ef,abc,de,ab')     as find_in_set_2,  //4
       find_in_set('at', 'ef,ab,de')         as find_in_set_3;
//0

-- 将字符串subject按照pattern正则表达式的规则拆分，返回index指定的字符。
select regexp_extract('foothebar', 'foo(.*?)(bar)', 1) as a, //the
       regexp_extract('foothebar', 'foo(.*?)(bar)', 2) as b, //bar
       regexp_extract('foothebar', 'foo(.*?)(bar)', 0) as c;
//foothebar

-- 'foothebar': 这是要搜索的原始字符串，即输入字符串。
-- 'foo(.*?)(bar)': 这是一个正则表达式，用于指定要匹配的字符串模式。它包含三个组件：
-- 'foo'：字符串 'foo'，它是要匹配的固定文本部分。
-- '(.*?)'：一个非贪婪的捕获组，用于匹配 'foo' 和 'bar' 之间的任意字符。
-- 'bar'：字符串 'bar'，它是要匹配的固定文本部分。

-- 1、2、0：这些是要从输入字符串中提取的捕获组的索引。在上面的正则表达式中，第一个捕获组是 (.*?)，第二个捕获组是 (bar)，因此 1 表示提取第一个捕获组，2 表示提取第二个捕获组，而 0 表示提取整个匹配的字符串（即包括 'foo' 和 'bar'）。
-- 根据上述解释，这个查询的结果如下：
-- regexp_extract('foothebar', 'foo(.*?)(bar)', 1)：这个函数提取了正则表达式中的第一个捕获组，即 '(.*?)'，然后返回从第一个 'foo' 到第一个 'bar' 之间的任意字符。在这个例子中，返回的字符串是 'the'。
-- regexp_extract('foothebar', 'foo(.*?)(bar)', 2)：这个函数提取了正则表达式中的第二个捕获组，即 '(bar)'，然后返回字符串 'bar'。
-- regexp_extract('foothebar', 'foo(.*?)(bar)', 0)：这个函数提取了整个正则表达式所匹配的字符串，包括 'foo' 和 'bar' 之间的任意字符。在这个例子中，返回的字符串是 'foothebar'。


-- 注意，在有些情况下要使用转义字符，下面的等号要用双竖线转义，这是java正则表达式的规则。
select data_field,
       regexp_extract(data_field, '.*?bgStart\\=([^&]+)', 1)                 as aaa,
       regexp_extract(data_field, '.*?contentLoaded_headStart\\=([^&]+)', 1) as bbb,
       regexp_extract(data_field, '.*?AppLoad2Req\\=([^&]+)', 1)             as ccc
from pt_nginx_loginlog_st
where pt = '2021-03-28'
limit 2;


-- data_field：这是要搜索的原始字符串，即输入字符串。
--
-- '.*?bgStart\\=([^&]+)'：
-- '.*?bgStart\\='：匹配任意字符零次或多次，直到遇到 'bgStart='
-- '([^&]+)'：匹配 'bgStart=' 后面的任意字符，直到遇到 '&' 或字符串结尾。
-- '\\=' 使用双斜杠进行转义，以表示等号字符 '='。
--
-- '1'：这是要从输入字符串中提取的捕获组的索引。在上面的正则表达式中，唯一的捕获组是 ([^&]+)，因此 1 表示提取第一个捕获组。
--
-- from pt_nginx_loginlog_st：这个子句指定要从哪个表中检索数据。
-- where pt = '2021-03-28'：这个子句指定要检索哪个日期的数据。
-- limit 2：这个子句指定要返回的最大行数。

-- 根据上述解释，这个查询的结果如下：
-- regexp_extract(data_field, '.*?bgStart\\=([^&]+)', 1) as aaa：这个函数提取了正则表达式中的第一个捕获组，即 ([^&]+)，然后返回 'bgStart=' 后面的任意字符，直到遇到 '&' 或字符串结尾。在这个例子中，返回的字符串是 '1616955596866'。
-- regexp_extract(data_field, '.*?contentLoaded_headStart\\=([^&]+)', 1) as bbb：这个函数提取了正则表达式中的第一个捕获组，即 ([^&]+)，然后返回 'contentLoaded_headStart=' 后面的任意字符，直到遇到 '&' 或字符串结尾。在这个例子中，返回的字符串是 '1616955596866'。
-- regexp_extract(data_field, '.*?AppLoad2Req\\=([^&]+)', 1) as ccc：这个函数提取了正则表达式中的第一个捕获组，即 ([^&]+)，然后返回 'AppLoad2Req=' 后面的任意字符，直到遇到 '&' 或字符串结尾。在这个例子中，返回的字符串是 '1616955596866'。
-- 需要注意的是，如果输入字符串不包含指定的子字符串，则 regexp_extract 函数返回 NULL。此外，如果输入字符串不符合指定的正则表达式，则可能会引发解析错误。


-- 说明：返回URL中指定的部分。partToExtract的有效值为：HOST, PATH, QUERY, REF, PROTOCOL, AUTHORITY, FILE, and USERINFO
select parse_url('https://www.tableName.com/path1/p.php?k1=v1&k2=v2#Ref1', 'HOST') as parse_url; //www.tableName.com

select parse_url('https://www.tableName.com/path1/p.php?k1=v1&k2=v2#Ref1', 'QUERY', 'k1') as parse_url;
//v1


-- HOST：从 URL 中提取主机名部分（不包括端口号）。
-- SELECT parse_url('https://www.example.com:8080/path/to/file?query=123', 'HOST') AS result;
-- -- 输出：www.example.com
--
--
-- PATH：从 URL 中提取路径部分（不包括查询参数和片段标识符）。
-- SELECT parse_url('https://www.example.com:8080/path/to/file?query=123', 'PATH') AS result;
-- -- 输出：/path/to/file
--
--
-- QUERY：从 URL 中提取查询参数部分（即问号后面的部分）。
-- SELECT parse_url('https://www.example.com:8080/path/to/file?query=123', 'QUERY') AS result;
-- -- 输出：query=123
--
--
-- REF：从 URL 中提取片段标识符部分（即井号后面的部分）。
-- SELECT parse_url('https://www.example.com:8080/path/to/file#section1', 'REF') AS result;
-- -- 输出：section1
--
--
-- PROTOCOL：从 URL 中提取协议部分（即冒号和双斜线之间的部分）。
-- SELECT parse_url('https://www.example.com:8080/path/to/file?query=123', 'PROTOCOL') AS result;
-- -- 输出：https
--
--
-- AUTHORITY：从 URL 中提取授权部分（包括用户名、密码、主机名和端口号）。
-- SELECT parse_url('https://user:password@www.example.com:8080/path/to/file?query=123', 'AUTHORITY') AS result;
-- -- 输出：user:password@www.example.com:8080
--
--
-- FILE：从 URL 中提取文件名部分（即路径中的最后一个部分）。
-- SELECT parse_url('https://www.example.com:8080/path/to/file?query=123', 'FILE') AS result;
-- -- 输出：file
--
--
-- USERINFO：从 URL 中提取用户名和密码部分（如果有）。
-- SELECT parse_url('https://user:password@www.example.com:8080/path/to/file?query=123', 'USERINFO') AS result;
-- -- 输出：user:password
--
-- 需要注意的是，如果 URL 字符串不包含指定的部分，则 parse_url 函数返回 NULL。此外，如果 URL 字符串不符合标准格式，则可能会引发解析错误。


-- 说明：解析json的字符串json_string,返回path指定的内容。如果输入的json字符串无效，那么返回NULL。
select get_json_object(
               '{"store":{"fruit":\[{"weight":8,"type":"apple"},{"weight":9,"type":"pear"}], "bicycle":{"price":19.95,"color":"red"} },"email":"amy@only_for_json_udf_test.net","owner":"amy"}',
               '$.store') as get_json_object; //{"fruit":[{"weight":8,"type":"apple"},{"weight":9,"type":"pear"}],"bicycle":{"price":19.95,"color":"red"}}

select get_json_object(
               '{"store":{"fruit":\[{"weight":8,"type":"apple"},{"weight":9,"type":"pear"}], "bicycle":{"price":19.95,"color":"red"} },"email":"amy@only_for_json_udf_test.net","owner":"amy"}',
               '$.store.fruit[0].type') as get_json_object;
//apple

-- 9. 复合类型构建操作
-- (1) map
Create table mapTable as
select map('100', 'tom', '200', 'mary') as t;

describe mapTable;

-- t,"map<string,string>",""

select t
from mapTable;

-- {"100":"tom","200":"mary"}

-- (2) struct
create table struct_table as
select struct('tom', 'mary', 'tim') as t;

desc struct_table;

-- t,"struct<col1:string,col2:string,col3:string>",""

select t
from struct_table;

-- {"col1":"tom","col2":"mary","col3":"tim"}

-- (3) array
create table arr_table as
select array("tom", "mary", "tim") as t;

describe arr_table;

-- t,array<string>,""

select t
from arr_table;

-- ["tom","mary","tim"]

-- 10. 复杂类型访问操作
-- (1) array
drop table arr_table2;

create table arr_table2 as
select array("tom", "mary", "tim") as t;

select t[0], t[1]
from arr_table2;

-- tom,     mary

-- (2) map
Create table map_table2 as
select map('100', 'tom', '200', 'mary') as t;

select t['200'], t['100']
from map_table2;

-- mary,    tom

-- (3) struct
create table str_table2 as
select struct('tom', 'mary', 'tim') as t;

select t.col1, t.col3
from str_table2;

-- tom,     tim

-- 11. 复杂类型长度统计函数
-- (1) size: 返回map类型的长度
select size(t)
from map_table2;

-- 2

select size(t)
from arr_table2;

-- 3

-- (2) cast
select cast('1' as bigint) as cast_num;

-- 1

-- hive当中的lateral view 与 explode以及reflect
-- 1. 使用explode函数将hive表中的Map和Array字段数据进行拆分
create database hive_explode;

use hive_explode;

drop table t3;

create table t3
(
    name     string,
    children array<string>,
    address  Map<string,string>
) row format delimited fields terminated by '\t'
    collection items terminated by ','
    map keys terminated by ':'
    stored as textFile;

load data local inpath '/opt/module/datas/maparray' into table t3;

select *
from t3;

-- zhangsan,    "[""child1"",""child2"",""child3"",""child4""]",        "{""k1"":""v1"",""k2"":""v2""}"
-- lisi,        "[""child5"",""child6"",""child7"",""child8""]",        "{""k3"":""v3"",""k4"":""v4""}"

-- 拆分children
select explode(children) as myChild
from t3;

-- child1
-- child2
-- child3
-- child4
-- child5
-- child6
-- child7
-- child8

-- 拆分address
select explode(address) as (myAddressKey, myAddressValue)
from t3;

-- k1,v1
-- k2,v2
-- k3,v3
-- k4,v4

-- 2. 使用explode拆分json字符串
CREATE TABLE explode_lateral_view
(
    `area`      string,
    `goods_id`  string,
    `sale_info` string
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
    STORED AS textfile;

load data local inpath '/opt/module/datas/explode_json' overwrite into table explode_lateral_view;

select *
from explode_lateral_view;

select explode(split(goods_id, ',')) as explode_arr
from explode_lateral_view;

-- 1
-- 2
-- 3
-- 4
-- 5
-- 6
-- 7
-- 8
-- 9

select explode(split(area, ',')) as explode_map
from explode_lateral_view;

-- a:shandong
-- b:beijing
-- c:hebei

-- 拆解json字段
select explode(split(regexp_replace(regexp_replace(sale_info, '\\[\\{', ''), '}]', ''), '},\\{')) as sale_info
from explode_lateral_view;

-- regexp_replace(sale_info, '\\[\\{', '')：使用正则表达式将 sale_info 字段中的所有 [{ 替换为空字符串，去掉 JSON 数组的起始符号。
-- regexp_replace(..., '}]', '')：使用正则表达式将上一步操作后的结果中的所有 }] 替换为空字符串，去掉 JSON 数组的结束符号。
-- split(..., '},\\{')：使用 , 分隔符将上一步操作后的结果拆分成多个 JSON 对象，并将拆分后的结果作为数组返回。
-- explode(...)：将数组中的每个元素都转换成单独的行，每行包含一个 JSON 对象。

-- """source"":""7fresh"",""monthSales"":4900,""userCount"":1900,""score"":""9.9"""
-- """source"":""jd"",""monthSales"":2090,""userCount"":78981,""score"":""9.8"""
-- """source"":""jdmart"",""monthSales"":6987,""userCount"":1600,""score"":""9.0"""

-- 用get_json_object来获取key为monthSales的数据：

select get_json_object(explode(split(regexp_replace(regexp_replace(sale_info, '\\[\\{', ''), '}]', ''), '},\\{')),
                       '$.monthSales') as sale_info
from explode_lateral_view;


-- 然后挂了FAILED: SemanticException [Error 10081]: UDTF's are not supported outside the SELECT clause, nor nested in expressions
-- UDTF explode不能写在别的函数内
-- 如果你这么写，想查两个字段，

select explode(split(area, ',')) as area, good_id
from explode_lateral_view;

-- 会报错FAILED: SemanticException 1:40 Only a single expression in the SELECT clause is supported with UDTF's. Error encountered near token 'good_id'
-- 使用UDTF的时候，只支持一个字段，这时候就需要LATERAL VIEW出场了

-- 3. 配合LATERAL  VIEW使用
select *
from explode_lateral_view;

-- "a:shandong,b:beijing,c:hebei",
-- "1,2,3,4,5,6,7,8,9",
-- "[{""source"":""7fresh"",""monthSales"":4900,""userCount"":1900,""score"":""9.9""},{""source"":""jd"",""monthSales"":2090,""userCount"":78981,""score"":""9.8""},{""source"":""jdmart"",""monthSales"":6987,""userCount"":1600,""score"":""9.0""}]"

select goods_id2, sale_info
from explode_lateral_view
         LATERAL VIEW explode(split(goods_id, ',')) goods as goods_id2;

-- 1,"[{""source"":""7fresh"",""monthSales"":4900,""userCount"":1900,""score"":""9.9""},{""source"":""jd"",""monthSales"":2090,""userCount"":78981,""score"":""9.8""},{""source"":""jdmart"",""monthSales"":6987,""userCount"":1600,""score"":""9.0""}]"
-- 2,"[{""source"":""7fresh"",""monthSales"":4900,""userCount"":1900,""score"":""9.9""},{""source"":""jd"",""monthSales"":2090,""userCount"":78981,""score"":""9.8""},{""source"":""jdmart"",""monthSales"":6987,""userCount"":1600,""score"":""9.0""}]"
-- 3,"[{""source"":""7fresh"",""monthSales"":4900,""userCount"":1900,""score"":""9.9""},{""source"":""jd"",""monthSales"":2090,""userCount"":78981,""score"":""9.8""},{""source"":""jdmart"",""monthSales"":6987,""userCount"":1600,""score"":""9.0""}]"
-- 4,"[{""source"":""7fresh"",""monthSales"":4900,""userCount"":1900,""score"":""9.9""},{""source"":""jd"",""monthSales"":2090,""userCount"":78981,""score"":""9.8""},{""source"":""jdmart"",""monthSales"":6987,""userCount"":1600,""score"":""9.0""}]"
-- 5,"[{""source"":""7fresh"",""monthSales"":4900,""userCount"":1900,""score"":""9.9""},{""source"":""jd"",""monthSales"":2090,""userCount"":78981,""score"":""9.8""},{""source"":""jdmart"",""monthSales"":6987,""userCount"":1600,""score"":""9.0""}]"
-- 6,"[{""source"":""7fresh"",""monthSales"":4900,""userCount"":1900,""score"":""9.9""},{""source"":""jd"",""monthSales"":2090,""userCount"":78981,""score"":""9.8""},{""source"":""jdmart"",""monthSales"":6987,""userCount"":1600,""score"":""9.0""}]"
-- 7,"[{""source"":""7fresh"",""monthSales"":4900,""userCount"":1900,""score"":""9.9""},{""source"":""jd"",""monthSales"":2090,""userCount"":78981,""score"":""9.8""},{""source"":""jdmart"",""monthSales"":6987,""userCount"":1600,""score"":""9.0""}]"
-- 8,"[{""source"":""7fresh"",""monthSales"":4900,""userCount"":1900,""score"":""9.9""},{""source"":""jd"",""monthSales"":2090,""userCount"":78981,""score"":""9.8""},{""source"":""jdmart"",""monthSales"":6987,""userCount"":1600,""score"":""9.0""}]"
-- 9,"[{""source"":""7fresh"",""monthSales"":4900,""userCount"":1900,""score"":""9.9""},{""source"":""jd"",""monthSales"":2090,""userCount"":78981,""score"":""9.8""},{""source"":""jdmart"",""monthSales"":6987,""userCount"":1600,""score"":""9.0""}]"

-- 其中LATERAL VIEW explode(split(goods_id,','))goods相当于一个虚拟表，与原表explode_lateral_view笛卡尔积关联

-- 多重使用
select goods_id2, sale_info, area2
from explode_lateral_view
         LATERAL VIEW explode(split(goods_id, ',')) goods as goods_id2
         LATERAL VIEW explode(split(area, ',')) area as area2;

-- 1,"[...,...,...]",a:shandong
-- 1,"[...,...,...]",b:beijing
-- 1,"[...,...,...]",c:hebei
-- 2,"[...,...,...]",a:shandong
-- 2,"[...,...,...]",b:beijing
-- 2,"[...,...,...]",c:hebei
-- 3,"[...,...,...]",a:shandong
-- 3,"[...,...,...]",b:beijing
-- 3,"[...,...,...]",c:hebei
-- 4,"[...,...,...]",a:shandong
-- 4,"[...,...,...]",b:beijing
-- 4,"[...,...,...]",c:hebei
-- 5,"[...,...,...]",a:shandong
-- 5,"[...,...,...]",b:beijing
-- 5,"[...,...,...]",c:hebei
-- 6,"[...,...,...]",a:shandong
-- 6,"[...,...,...]",b:beijing
-- 6,"[...,...,...]",c:hebei
-- 7,"[...,...,...]",a:shandong
-- 7,"[...,...,...]",b:beijing
-- 7,"[...,...,...]",c:hebei
-- 8,"[...,...,...]",a:shandong
-- 8,"[...,...,...]",b:beijing
-- 8,"[...,...,...]",c:hebei
-- 9,"[...,...,...]",a:shandong
-- 9,"[...,...,...]",b:beijing
-- 9,"[...,...,...]",c:hebei

-- 三个表笛卡尔积的结果

-- 通过下面的句子，把这个json格式的一行数据，完全转换成二维表的方式展现
select get_json_object(concat('{', sale_info_1, '}'), '$.source')     as source,
       get_json_object(concat('{', sale_info_1, '}'), '$.monthSales') as monthSales,
       get_json_object(concat('{', sale_info_1, '}'), '$.userCount')  as monthSales,
       get_json_object(concat('{', sale_info_1, '}'), '$.score')      as monthSales
from explode_lateral_view LATERAL VIEW explode(
        split(regexp_replace(regexp_replace(sale_info, '\\[\\{', ''), '}]', ''), '},\\{')) sale_info as sale_info_1;

-- 7fresh,    4900,    1900,     9.9
-- jd,        2090,    78981,    9.8
-- jdmart,    6987,    1600,     9.0

-- 总结:
-- (1) Lateral View通常和UDTF一起出现，为了解决UDTF不允许在select字段的问题。
-- (2) Multiple Lateral View可以实现类似笛卡尔乘积。
-- (3) Outer关键字可以把不输出的UDTF的空结果，输出成NULL，防止丢失数据。

-- UDTF（User-Defined Table-Generating Function），也就是用户自定义的函数，它可以接受一些参数，并返回一个表格形式的结果。
-- 然而，在 SQL 中，通常是不允许在 SELECT 字段中使用 UDTF ，因为 SELECT 字段必须是一个标量值，而不是一个表格形式的结果。
-- 为了解决这个问题，Hive 引入了 Lateral View 语法，它可以将 UDTF 的结果展开成多行，从而可以在 SELECT 字段中使用。

drop table test;

create table test
(
    a array<int> comment ''
) comment ''
    row format delimited fields terminated by '|'
        collection items terminated by ','
    stored as textfile;

load data local inpath '/opt/module/datas/array' into table test;

select *
from test
         lateral view explode(a) tmp as a;

-- "[1,2,3]",   1
-- "[1,2,3]",   2
-- "[1,2,3]",   3

select *
from test
         lateral view explode(`array`()) tmp as a;

-- 什么也不显示

select *
from test
         lateral view outer explode(`array`()) tmp as a;

-- "[1,2,3]",   <null>
-- 加outer之后null也显示了

-- 行转列
drop table person_info;

create table person_info
(
    name          string,
    constellation string,
    blood_type    string
)
    row format delimited fields terminated by "\t";

load data local inpath '/opt/module/datas/constellation.txt' into table person_info;

select *
from person_info;

-- 孙悟空,    白羊座,      A
-- 老王,      射手座,     A
-- 宋宋,      白羊座,     B
-- 猪八戒,    白羊座,     A
-- 凤姐,     射手座,      A

select t1.base,
       concat_ws('|', collect_set(t1.name)) name
from (select name,
             concat(constellation, ",", blood_type) base
      from person_info) t1
group by t1.base;

-- "射手座,A",     老王|凤姐
-- "白羊座,A",     孙悟空|猪八戒
-- "白羊座,B",     宋宋

-- 列转行
create table movie_info
(
    movie    string,
    category array<string>
)
    row format delimited fields terminated by "\t"
        collection items terminated by ",";

load data local inpath "/opt/module/datas/movie.txt" into table movie_info;

select *
from movie_info;

-- 《疑犯追踪》,         "[""悬疑"",""动作"",""科幻"",""剧情""]"
-- 《Lie to me》,      "[""悬疑"",""警匪"",""动作"",""心理"",""剧情""]"
-- 《战狼2》,           "[""战争"",""动作"",""灾难""]"

select movie,
       type
from movie_info
         lateral view explode(category) tmp as type;

-- 《疑犯追踪》,       悬疑
-- 《疑犯追踪》,       动作
-- 《疑犯追踪》,       科幻
-- 《疑犯追踪》,       剧情
-- 《Lie to me》,     悬疑
-- 《Lie to me》,     警匪
-- 《Lie to me》,     动作
-- 《Lie to me》,     心理
-- 《Lie to me》,     剧情
-- 《战狼2》,         战争
-- 《战狼2》,         动作
-- 《战狼2》,         灾难

-- reflect函数: reflect函数可以支持在sql中调用java中的自带函数，秒杀一切udf函数。
create table test_udf
(
    col1 int,
    col2 int
) row format delimited fields terminated by ',';

load data local inpath '/opt/module/datas/test_udf' overwrite into table test_udf;

select *
from test_udf;

-- 1,    2
-- 4,    3
-- 6,    4
-- 7,    5
-- 5,    6

-- 使用java.lang.Math当中的Max求两列当中的最大值
select reflect("java.lang.Math", "max", col1, col2)
from test_udf;

-- 2
-- 4
-- 6
-- 7
-- 6

select reflect("java.lang.Math", "min", col1, col2)
from test_udf;

-- 1
-- 3
-- 4
-- 5
-- 5

-- 文件中不同的记录来执行不同的java的内置函数
create table test_udf2
(
    class_name  string,
    method_name string,
    col1        int,
    col2        int
) row format delimited fields terminated by ',';

load data local inpath '/opt/module/datas/test_udf2' overwrite into table test_udf2;

select *
from test_udf2;

-- java.lang.Math,    min,    1,    2
-- java.lang.Math,    max,    2,    3

select reflect(class_name, method_name, col1, col2)
from test_udf2;

-- 判断是否为数字
-- 使用apache commons中的函数，commons下的jar已经包含在hadoop的classpath中，所以可以直接使用。
select reflect("org.apache.commons.lang.math.NumberUtils", "isNumber", "123");

-- true