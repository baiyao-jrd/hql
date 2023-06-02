-- hql 高级题
set hive.execution.engine = spark;

-- 1. 各个视频的平均完播率
--      计算2021年里有播放记录的每个视频的完播率（保留三位小数），按照完播率降序排序
--      完播率：完成播放次数占总播放次数的比例 -> 简单起见 -> 结束观看时间与开始播放时间的差 >= 视频时长 -> 视为完播

drop table if exists k1_user_video_log;

drop table if exists k2_video_info;

create table if not exists k1_user_video_log
(
    uid        int,
    video_id   int,
    start_time timestamp,
    end_time   timestamp,
    if_follow  tinyint,
    if_like    tinyint,
    if_retweet tinyint,
    comment_id int
) row format delimited fields terminated by ','
    stored as textfile;

create table if not exists k2_video_info
(
    video_id     int,
    author       int,
    tag          string,
    duration     int,
    release_time timestamp
) row format delimited fields terminated by ','
    stored as textfile;

INSERT INTO k1_user_video_log(uid, video_id, start_time, end_time, if_follow, if_like, if_retweet, comment_id)
VALUES (101, 2001, '2021-10-01 10:00:00', '2021-10-01 10:00:30', 0, 1, 1, null),
       (102, 2001, '2021-10-01 10:00:00', '2021-10-01 10:00:24', 0, 0, 1, null),
       (103, 2001, '2021-10-01 11:00:00', '2021-10-01 11:00:34', 0, 1, 0, 1732526),
       (101, 2002, '2021-09-01 10:00:00', '2021-09-01 10:00:42', 1, 0, 1, null),
       (102, 2002, '2021-10-01 11:00:00', '2021-10-01 11:00:30', 1, 0, 1, null);

INSERT INTO k2_video_info(video_id, author, tag, duration, release_time)
VALUES (2001, 901, '影视', 30, '2021-01-01 7:00:00'),
       (2002, 901, '美食', 60, '2021-01-01 7:00:00'),
       (2003, 902, '旅游', 90, '2021-01-01 7:00:00');

-- 用户视频互动表
select *
from k1_user_video_log;

-- 段视频信息表
select *
from k2_video_info;

select k1.video_id,
       round(count(if(unix_timestamp(end_time) - unix_timestamp(start_time) >= duration, 1, null)) / count(*),
             3) as rate
from k1_user_video_log k1
         left join k2_video_info k2vi on k1.video_id = k2vi.video_id
group by k1.video_id
order by rate desc;

select unix_timestamp('2021-10-01 10:00:00') - unix_timestamp('2021-10-01 10:00:30') as test1;

-- 2. 平均播放进度大于60%的视频类别
--      计算各类视频的平均播放进度，将进度大于60%的类别输出
--      播放进度 = 播放时长 ÷ 视频时长 × 100%, 当播放时长大于视频时长的时候,播放进度均记为100%
--      结果保留两位小数,按照播放进度倒序排序

create table if not exists k3_user_video_log
(
    uid        int,
    video_id   int,
    start_time timestamp,
    end_time   timestamp,
    if_follow  tinyint,
    if_like    tinyint,
    if_retweet tinyint,
    comment_id int
) row format delimited fields terminated by '\t'
    stored as textfile;

drop table if exists k4_video_info;

create table if not exists k4_video_info
(
    video_id     int,
    author       int,
    tag          string,
    duration     int,
    release_time timestamp
) row format delimited fields terminated by '\t'
    stored as textfile;


INSERT INTO k3_user_video_log(uid, video_id, start_time, end_time, if_follow, if_like, if_retweet, comment_id)
VALUES (101, 2001, '2021-10-01 10:00:00', '2021-10-01 10:00:30', 0, 1, 1, null),
       (102, 2001, '2021-10-01 10:00:00', '2021-10-01 10:00:21', 0, 0, 1, null),
       (103, 2001, '2021-10-01 11:00:50', '2021-10-01 11:01:20', 0, 1, 0, 1732526),
       (102, 2002, '2021-10-01 11:00:00', '2021-10-01 11:00:30', 1, 0, 1, null),
       (103, 2002, '2021-10-01 10:59:05', '2021-10-01 11:00:05', 1, 0, 1, null);

INSERT INTO k4_video_info(video_id, author, tag, duration, release_time)
VALUES (2001, 901, '影视', 30, '2021-01-01 7:00:00'),
       (2002, 901, '美食', 60, '2021-01-01 7:00:00'),
       (2003, 902, '旅游', 90, '2020-01-01 7:00:00');

-- 用户视频互动表
select *
from k3_user_video_log;

-- 短视频信息表
select *
from k4_video_info;

select tag,
       avg(`if`((unix_timestamp(end_time) - unix_timestamp(start_time)) >= duration, 1.0 * 100,
                round((unix_timestamp(end_time) - unix_timestamp(start_time)) / duration * 100, 2))) as progress
from k3_user_video_log k3
         left join k4_video_info k4vi on k3.video_id = k4vi.video_id
group by tag
having progress > 0.6 * 100
order by progress desc;

-- 3. 每类视频近一个月的转发量和转发率
--      统计在有用户互动的最近一个月中（按照包含当天在内的近30天算，比如10月31号的近30天 -> 10.2 ~ 10.31 之间的数据）
--      每类视频的转发量和转发率
--      转发率 = 转发量 ÷ 播放量 -> 结果按照转发率降序排序

drop table if exists k5_user_video_log;

drop table if exists k6_video_info;

create table if not exists k5_user_video_log
(
    uid        int,
    video_id   int,
    start_time timestamp,
    end_time   timestamp,
    if_follow  tinyint,
    if_like    tinyint,
    if_retweet tinyint,
    comment_id int
) row format delimited fields terminated by '\t'
    stored as textfile;

create table if not exists k6_video_info
(
    video_id     int,
    author       int,
    tag          string,
    duration     int,
    release_time timestamp
) row format delimited fields terminated by '\t'
    stored as textfile;

INSERT INTO k5_user_video_log(uid, video_id, start_time, end_time, if_follow, if_like, if_retweet, comment_id)
VALUES (101, 2001, '2021-10-01 10:00:00', '2021-10-01 10:00:20', 0, 1, 1, null)
     , (102, 2001, '2021-10-01 10:00:00', '2021-10-01 10:00:15', 0, 0, 1, null)
     , (103, 2001, '2021-10-01 11:00:50', '2021-10-01 11:01:15', 0, 1, 0, 1732526)
     , (102, 2002, '2021-09-10 11:00:00', '2021-09-10 11:00:30', 1, 0, 1, null)
     , (103, 2002, '2021-10-01 10:59:05', '2021-10-01 11:00:05', 1, 0, 0, null);

INSERT INTO k6_video_info(video_id, author, tag, duration, release_time)
VALUES (2001, 901, '影视', 30, '2021-01-01 7:00:00')
     , (2002, 901, '美食', 60, '2021-01-01 7:00:00')
     , (2003, 902, '旅游', 90, '2020-01-01 7:00:00');

-- 用户视频互动表
select *
from k5_user_video_log;

-- 短视频信息表
select *
from k6_video_info;

select tag,
       sum(if_retweet)                            as retweet_total,
       round(sum(if_retweet) / count(*) * 100, 2) as retweet_rate
from k5_user_video_log k5
         left join k6_video_info k6 on k5.video_id = k6.video_id
where date_format(start_time, 'yyyy-MM-dd') >= date_sub('2021-10-01', 29)
group by tag
order by retweet_rate desc;

-- 4. 每个创作者每月的涨粉率及截止当前的总粉丝量
--      计算2021年里每个创作者每月的涨粉率及截至当月的总粉丝量
--      涨粉率 = （加粉量 - 掉粉量） / 播放量
--      结果按照创作者id、总粉丝量升序排序
--      if_follow -> 是否关注
--      为0 -> 表示此次互动前后关注状态未发生变化
--      为1 -> 表示用户观看视频中关注了视频创作者
--      为2 -> 表示本次观看过程中取消了关注

drop table if exists k7_user_video_log;

drop table if exists k8_video_info;

create table if not exists k7_user_video_log
(
    uid        int,
    video_id   int,
    start_time timestamp,
    end_time   timestamp,
    if_follow  tinyint,
    if_like    tinyint,
    if_retweet tinyint,
    comment_id int
) row format delimited fields terminated by ','
    stored as textfile;

create table if not exists k8_video_info
(
    video_id     int,
    author       int,
    tag          string,
    duration     int,
    release_time timestamp
) row format delimited fields terminated by ','
    stored as textfile;

INSERT INTO k7_user_video_log(uid, video_id, start_time, end_time, if_follow, if_like, if_retweet, comment_id)
VALUES (101, 2001, '2021-09-01 10:00:00', '2021-09-01 10:00:20', 0, 1, 1, null)
     , (105, 2002, '2021-09-10 11:00:00', '2021-09-10 11:00:30', 1, 0, 1, null)
     , (101, 2001, '2021-10-01 10:00:00', '2021-10-01 10:00:20', 1, 1, 1, null)
     , (102, 2001, '2021-10-01 10:00:00', '2021-10-01 10:00:15', 0, 0, 1, null)
     , (103, 2001, '2021-10-01 11:00:50', '2021-10-01 11:01:15', 1, 1, 0, 1732526)
     , (106, 2002, '2021-10-01 10:59:05', '2021-10-01 11:00:05', 2, 0, 0, null);

INSERT INTO k8_video_info(video_id, author, tag, duration, release_time)
VALUES (2001, 901, '影视', 30, '2021-01-01 7:00:00')
     , (2002, 901, '影视', 60, '2021-01-01 7:00:00')
     , (2003, 902, '旅游', 90, '2020-01-01 7:00:00')
     , (2004, 902, '美女', 90, '2020-01-01 8:00:00');

-- 用户视频互动表
select *
from k7_user_video_log;

-- 视频信息表
select *
from k8_video_info;

with temp_table as (
    select k7.video_id,
           start_time,
           author,
           tag,
           if_follow
    from k7_user_video_log k7
             left join k8_video_info k8 on k7.video_id = k8.video_id
)
select distinct temp_1.year_month,
                temp_1.author,
                inc_fans_rate,
                fans_total
from (
         select author,
                date_format(start_time, 'yyyy-MM') as year_month,
                round((sum(`if`(if_follow = 1, 1, 0)) - sum(`if`(if_follow = 2, 1, 0))) / count(*) * 100,
                      2)                           as inc_fans_rate
         from temp_table
         group by author, date_format(start_time, 'yyyy-MM')
     ) as temp_1
         left join (
    select author,
           year_month,
           fans_total_1 - fans_total_2 as fans_total
    from (
             select author,
                    date_format(start_time, 'yyyy-MM')                                             as year_month,
                    sum(`if`(if_follow = 1, 1, 0))
                        over (partition by author order by date_format(start_time, 'yyyy-MM') asc) as fans_total_1,
                    sum(`if`(if_follow = 2, 1, 0))
                        over (partition by author order by date_format(start_time, 'yyyy-MM') asc) as fans_total_2
             from temp_table
         ) as table_temp
) temp_2
                   on temp_1.author = temp_2.author and temp_1.year_month = temp_2.year_month
order by temp_1.author asc, fans_total asc;

-- 5. 近一个月发布的视频中热度最高的top3视频
--      找出近一个月发布的视频中热度最高的top3视频
--      热度 = (a * 视频完播率 + b * 点赞数 + c * 评论数 + d * 转发数) * 新鲜度
--      新鲜度 = 1 / (最近无播放天数 + 1)
--      当前配置的参数a, b, c, d -> 100, 5, 3, 2
--      最近播放日期以end_time-结束观看时间为准，假设T，则近一个月按[T-29, T]闭区间统计
--      结果中热度保留为整数，并按热度降序排序

drop table if exists k11_user_video_log;

drop table if exists k12_video_info;

create table if not exists k11_user_video_log
(
    uid        int,
    video_id   int,
    start_time timestamp,
    end_time   timestamp,
    if_follow  tinyint,
    if_like    tinyint,
    if_retweet tinyint,
    comment_id int
) row format delimited fields terminated by ','
    stored as textfile;

create table if not exists k12_video_info
(
    video_id     int,
    author       int,
    tag          string,
    duration     int,
    release_time timestamp
) row format delimited fields terminated by ','
    stored as textfile;

INSERT INTO k11_user_video_log(uid, video_id, start_time, end_time, if_follow, if_like, if_retweet, comment_id)
VALUES (101, 2001, '2021-09-24 10:00:00', '2021-09-24 10:00:20', 1, 1, 0, null)
     , (105, 2002, '2021-09-25 11:00:00', '2021-09-25 11:00:30', 0, 0, 1, null)
     , (102, 2002, '2021-09-25 11:00:00', '2021-09-25 11:00:30', 1, 1, 1, null)
     , (101, 2002, '2021-09-26 11:00:00', '2021-09-26 11:00:30', 1, 0, 1, null)
     , (101, 2002, '2021-09-27 11:00:00', '2021-09-27 11:00:30', 1, 1, 0, null)
     , (102, 2002, '2021-09-28 11:00:00', '2021-09-28 11:00:30', 1, 0, 1, null)
     , (103, 2002, '2021-09-29 11:00:00', '2021-09-29 11:00:30', 1, 0, 1, null)
     , (102, 2002, '2021-09-30 11:00:00', '2021-09-30 11:00:30', 1, 1, 1, null)
     , (101, 2001, '2021-10-01 10:00:00', '2021-10-01 10:00:20', 1, 1, 0, null)
     , (102, 2001, '2021-10-01 10:00:00', '2021-10-01 10:00:15', 0, 0, 1, null)
     , (103, 2001, '2021-10-01 11:00:50', '2021-10-01 11:01:15', 1, 1, 0, 1732526)
     , (106, 2002, '2021-10-02 10:59:05', '2021-10-02 11:00:05', 2, 0, 1, null)
     , (107, 2002, '2021-10-02 10:59:05', '2021-10-02 11:00:05', 1, 0, 1, null)
     , (108, 2002, '2021-10-02 10:59:05', '2021-10-02 11:00:05', 1, 1, 1, null)
     , (109, 2002, '2021-10-03 10:59:05', '2021-10-03 11:00:05', 0, 1, 0, null);

INSERT INTO k12_video_info(video_id, author, tag, duration, release_time)
VALUES (2001, 901, '旅游', 30, '2020-01-01 7:00:00')
     , (2002, 901, '旅游', 60, '2021-01-01 7:00:00')
     , (2003, 902, '影视', 90, '2020-01-01 7:00:00')
     , (2004, 902, '美女', 90, '2020-01-01 8:00:00');

-- 用户视频互动表
select *
from k11_user_video_log;

-- 短视频信息表
select *
from k12_video_info;

select tag,
       k11.video_id,
       round(count(`if`((unix_timestamp(end_time) - unix_timestamp(start_time)) >= duration, 1, null)) / count(*) *
             100 + 5 * sum(if_like) + 3 * count(comment_id) + sum(if_retweet) * 2 * (1 / (`if`(
                   datediff((select date_format(max(start_time), 'yyyy-MM-dd') from k11_user_video_log group by 1),
                            date_format(max(start_time), 'yyyy-MM-dd')) > 29, 29,
                   datediff((select date_format(max(start_time), 'yyyy-MM-dd') from k11_user_video_log group by 1),
                            date_format(max(start_time), 'yyyy-MM-dd')) + 1))), 0) as heat
from k11_user_video_log k11
         left join k12_video_info k12 on k11.video_id = k12.video_id
where date_format(start_time, 'yyyy-MM-dd') >=
      date_sub((select date_format(max(start_time), 'yyyy-MM-dd') from k11_user_video_log group by 1), 29)
group by tag, k11.video_id
order by heat desc
limit 3;

-- 6. 2021年11月每天的人均浏览文章时长
--      artical_id -> 代表用户浏览的文章id -> 为0, 表示用户在非文章内容页（例如，app内的列表页、活动页等）
--      统计2021年11月每天的人均浏览文章时长（秒数），结果保留一位小数，按照时长升序排序

drop table if exists m1_user_log;

create table if not exists m1_user_log
(
    uid        int comment '用户id',
    artical_id int comment '视频id',
    in_time    timestamp comment '进入时间',
    out_time   timestamp comment '离开时间',
    sign_in    tinyint comment '是否签到'
) comment '用户行为日志表'
    row format delimited fields terminated by ','
    stored as textfile;

INSERT INTO m1_user_log(uid, artical_id, in_time, out_time, sign_in)
VALUES (101, 9001, '2021-11-01 10:00:00', '2021-11-01 10:00:31', 0),
       (102, 9001, '2021-11-01 10:00:00', '2021-11-01 10:00:24', 0),
       (102, 9002, '2021-11-01 11:00:00', '2021-11-01 11:00:11', 0),
       (101, 9001, '2021-11-02 10:00:00', '2021-11-02 10:00:50', 0),
       (102, 9002, '2021-11-02 11:00:01', '2021-11-02 11:00:24', 0);

-- 用户行为日志表
select *
from m1_user_log;

select date_format(in_time, 'yyyy-MM-dd')                                                      as day,
       round(sum(unix_timestamp(out_time) - unix_timestamp(in_time)) / count(distinct uid), 1) as human_avg_duration
from m1_user_log
where date_format(in_time, 'yyyy-MM') = '2021-11'
  and artical_id != 0
group by date_format(in_time, 'yyyy-MM-dd')
order by human_avg_duration asc;

-- 7. 2021年11月每天新用户的次日留存率
--      保留两位小数
--      次日留存率为当天新增的用户数中第二天又活跃了的用户数占比
--      如果in_time进入时间和out_time离开时间跨天了，在两天里都记为该用户活跃过，结果按日期升序。

drop table if exists m3_user_log;

create table if not exists m3_user_log
(
    uid        int comment '用户id',
    artical_id int comment '视频id',
    in_time    timestamp comment '进入时间',
    out_time   timestamp comment '离开时间',
    sign_in    tinyint comment '是否签到'
) comment '用户行为日志表'
    row format delimited fields terminated by ','
    stored as textfile;

INSERT INTO m3_user_log(uid, artical_id, in_time, out_time, sign_in)
VALUES (101, 0, '2021-11-01 10:00:00', '2021-11-01 10:00:42', 1),
       (102, 9001, '2021-11-01 10:00:00', '2021-11-01 10:00:09', 0),
       (103, 9001, '2021-11-01 10:00:01', '2021-11-01 10:01:50', 0),
       (101, 9002, '2021-11-02 10:00:09', '2021-11-02 10:00:28', 0),
       (103, 9002, '2021-11-02 10:00:51', '2021-11-02 10:00:59', 0),
       (104, 9001, '2021-11-02 10:00:28', '2021-11-02 10:00:50', 0),
       (101, 9003, '2021-11-03 11:00:55', '2021-11-03 11:01:24', 0),
       (104, 9003, '2021-11-03 11:00:45', '2021-11-03 11:00:55', 0),
       (105, 9003, '2021-11-03 11:00:53', '2021-11-03 11:00:59', 0),
       (101, 9002, '2021-11-04 11:00:55', '2021-11-04 11:00:59', 0);

-- 用户行为日志表
select *
from m3_user_log;

select register_day,
       round(count(`if`(date_format(in_time, 'yyyy-MM-dd') = register_day and flag = 1, 1, null)) /
             count(`if`(date_format(in_time, 'yyyy-MM-dd') = register_day, 1, null)), 2) as rate
from m3_user_log a
         left join (
    select uid,
           min(ts)                                    as register_day,
           `if`(date_sub(max(ts), 1) = min(ts), 1, 0) as flag
    from (
             select uid,
                    ts,
                    dense_rank() over (partition by uid order by ts asc) as ranking
             from (
                      select uid,
                             date_format(in_time, 'yyyy-MM-dd') as ts
                      from m3_user_log
                      union all
                      select uid,
                             date_format(out_time, 'yyyy-MM-dd') as ts
                      from m3_user_log
                  ) as table_temp
         ) as temp_table
    where ranking <= 2
    group by uid
) as b
                   on a.uid = b.uid
group by register_day
order by register_day asc;

-- 8. 每天的日活数及新用户占比
--      新用户占比 = 当天的新用户数 ÷ 当天活跃用户数（日活数）
--      如果in_time进入时间和out_time离开时间跨天了，在两天里都记为该用户活跃过，结果按日期升序。

drop table if exists m5_user_log;

create table if not exists m5_user_log
(
    uid        int comment '用户id',
    artical_id int comment '视频id',
    in_time    timestamp comment '进入时间',
    out_time   timestamp comment '离开时间',
    sign_in    tinyint comment '是否签到'
) comment '用户行为日志表'
    row format delimited fields terminated by ','
    stored as textfile;

INSERT INTO m5_user_log(uid, artical_id, in_time, out_time, sign_in)
VALUES (101, 9001, '2021-10-31 10:00:00', '2021-10-31 10:00:09', 0),
       (102, 9001, '2021-10-31 10:00:00', '2021-10-31 10:00:09', 0),
       (101, 0, '2021-11-01 10:00:00', '2021-11-01 10:00:42', 1),
       (102, 9001, '2021-11-01 10:00:00', '2021-11-01 10:00:09', 0),
       (108, 9001, '2021-11-01 10:00:01', '2021-11-01 10:01:50', 0),
       (108, 9001, '2021-11-02 10:00:01', '2021-11-02 10:01:50', 0),
       (104, 9001, '2021-11-02 10:00:28', '2021-11-02 10:00:50', 0),
       (106, 9001, '2021-11-02 10:00:28', '2021-11-02 10:00:50', 0),
       (108, 9001, '2021-11-03 10:00:01', '2021-11-03 10:01:50', 0),
       (109, 9002, '2021-11-03 11:00:55', '2021-11-03 11:00:59', 0),
       (104, 9003, '2021-11-03 11:00:45', '2021-11-03 11:00:55', 0),
       (105, 9003, '2021-11-03 11:00:53', '2021-11-03 11:00:59', 0),
       (106, 9003, '2021-11-03 11:00:45', '2021-11-03 11:00:55', 0);

-- 用户行为日志表
select *
from m5_user_log;

with temp_table as (
    select uid, ts
    from (
             select uid,
                    date_format(in_time, 'yyyy-MM-dd') as ts
             from m5_user_log
             union all
             select uid,
                    date_format(out_time, 'yyyy-MM-dd') as ts
             from m5_user_log
         ) as table_temp
    group by uid, ts
)
select ts,
       count(distinct a.uid)                                                     as dau,
       round(count(`if`(register_day = ts, 1, null)) / count(distinct a.uid), 2) as rate
from temp_table a
         left join (
    select uid,
           min(ts) as register_day
    from temp_table
    group by uid
) as b
                   on a.uid = b.uid
group by ts
order by ts asc;

-- 9. 连续签到领金币
--      artical_id -> 文章id代表用户浏览的文章ID -> 文章id为0表示用户在非文章内容页（比如App内的列表页、活动页等）。
--      只有artical_id为0时sign_in值才有效。
--
--      2021年7月7日开始 -> 用户每天签到可领1金币 -> 可累积签到天数，连续签到第3、7天分别可额外领2、6金币。
--
--      连续签到7天后重新累积签到天数
--
--      问题：
--      计算每个用户2021年7月以来每月获得的金币数（该活动到10月底结束，11月1日开始的签到不再获得金币）。结果按月份、ID升序排序。
--      注：如果签到记录的in_time-进入时间和out_time-离开时间跨天了，也只记作in_time对应的日期签到了。

drop table if exists m6_user_log;

create table if not exists m6_user_log
(
    uid        int comment '用户id',
    artical_id int comment '视频id',
    in_time    timestamp comment '进入时间',
    out_time   timestamp comment '离开时间',
    sign_in    tinyint comment '是否签到'
) comment '用户行为日志表'
    row format delimited fields terminated by ','
    stored as textfile;

INSERT INTO m6_user_log(uid, artical_id, in_time, out_time, sign_in)
VALUES (101, 0, '2021-07-07 10:00:00', '2021-07-07 10:00:09', 1),
       (101, 0, '2021-07-08 10:00:00', '2021-07-08 10:00:09', 1),
       (101, 0, '2021-07-09 10:00:00', '2021-07-09 10:00:42', 1),
       (101, 0, '2021-07-10 10:00:00', '2021-07-10 10:00:09', 1),
       (101, 0, '2021-07-11 23:59:55', '2021-07-11 23:59:59', 1),
       (101, 0, '2021-07-12 10:00:28', '2021-07-12 10:00:50', 1),
       (101, 0, '2021-07-13 10:00:28', '2021-07-13 10:00:50', 1),
       (102, 0, '2021-10-01 10:00:28', '2021-10-01 10:00:50', 1),
       (102, 0, '2021-10-02 10:00:01', '2021-10-02 10:01:50', 1),
       (102, 0, '2021-10-03 11:00:55', '2021-10-03 11:00:59', 1),
       (102, 0, '2021-10-04 11:00:45', '2021-10-04 11:00:55', 0),
       (102, 0, '2021-10-05 11:00:53', '2021-10-05 11:00:59', 1),
       (102, 0, '2021-10-06 11:00:45', '2021-10-06 11:00:55', 1);

-- 用户行为日志表
select *
from m6_user_log;

with temp_table as (
    select uid,
           date_format(in_time, 'yyyy-MM-dd')                                             as ts,
           rank() over (partition by uid order by date_format(in_time, 'yyyy-MM-dd') asc) as ranking
    from m6_user_log
    where sign_in = 1
)
select year_month,
       uid,
       sum(bit_nums) over (partition by uid order by year_month asc) as bit_nums
from (
         select date_format(ts, 'yyyy-MM') as year_month,
                uid,
                sum(bit_nums)              as bit_nums
         from (
                  select uid,
                         ts,
                         case (date_format(ts, 'yyyy-MM-dd') between '2021-07-07' and '2021-10-31')
                             when ranking % 7 = 3 then 3
                             when ranking % 7 = 0 then 7
                             else 1
                             end as bit_nums
                  from temp_table
              ) as table_temp
         group by uid, date_format(ts, 'yyyy-MM')
     ) as table_month
order by year_month asc, uid asc;

-- 10. 计算商城中2021年每月的GMV
--      用户将购物车中多件商品一起下单时，订单总表会生成一个订单（此时为待付款，status订单状态为0）
--      用户支付完成，status状态为1，表示已付款
--      用户退款完成，status状态为2，表示已退款。 -> 同时，订单总表生成一条交易总金额为负值的记录
--      计算商城中2021年每月的GMV，输出GMV大于10W的每月GMV，值保留整数
--      GMV为已付款订单和未付款订单两者之和，结果按GMV升序排序

drop table if exists n1_order_overall;

create table if not exists n1_order_overall
(
    order_id     int comment '订单号',
    uid          int comment '用户id',
    event_time   timestamp comment '下单时间',
    total_amount decimal comment '订单总金额',
    total_cnt    int comment '订单商品总件数',
    status       tinyint comment '订单状态'
) comment '订单总表'
    row format delimited fields terminated by ','
    stored as textfile;

INSERT INTO n1_order_overall(order_id, uid, event_time, total_amount, total_cnt, `status`)
VALUES (301001, 101, '2021-10-01 10:00:00', 15900, 2, 1),
       (301002, 101, '2021-10-01 11:00:00', 15900, 2, 1),
       (301003, 102, '2021-10-02 10:00:00', 34500, 8, 0),
       (301004, 103, '2021-10-12 10:00:00', 43500, 9, 1),
       (301005, 105, '2021-11-01 10:00:00', 31900, 7, 1),
       (301006, 102, '2021-11-02 10:00:00', 24500, 6, 1),
       (391007, 102, '2021-11-03 10:00:00', -24500, 6, 2),
       (301008, 104, '2021-11-04 10:00:00', 55500, 12, 0);

-- 订单总表
select *
from n1_order_overall;

select date_format(event_time, 'yyyy-MM') as year_month,
       sum(total_amount)                  as GMV
from n1_order_overall
where date_format(event_time, 'yyyy') = '2021'
  and status != 2
group by date_format(event_time, 'yyyy-MM')
order by GMV asc;

-- 11.