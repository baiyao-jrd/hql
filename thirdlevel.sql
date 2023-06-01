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
