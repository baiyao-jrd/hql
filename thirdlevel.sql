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
having GMV > 100000
order by GMV asc;

-- 11. 某店铺的各商品毛利率以及店铺的整体毛利率
--      计算2021年10月以来店铺中商品毛利率大于24.9%的商品信息以及店铺整体毛利率。
--      商品毛利率 = （1 - 进价 / 平均单件售价） * 100 %
--      店铺毛利率=(1-总进价成本/总销售收入)*100%
--      结果先输出店铺毛利率，再按照商品id升序输出各商品毛利率，均保留1位小数。

drop table if exists n3_order_overall;

drop table if exists n4_product_info;

drop table if exists n5_order_detail;

create table if not exists n3_order_overall
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

create table if not exists n4_product_info
(
    product_id   int comment '商品id',
    shop_id      int comment '店铺id',
    tag          string comment '商品类别标签',
    in_price     decimal comment '进货价格',
    quantity     int comment '进货数量',
    release_time timestamp comment '上架时间'
) comment '商品信息表'
    row format delimited fields terminated by ','
    stored as textfile;

create table if not exists n5_order_detail
(
    order_id   int comment '订单号',
    product_id int comment '商品id',
    price      decimal comment '商品单价',
    cnt        int comment '下单数量'
) comment '订单明细表'
    row format delimited fields terminated by ','
    stored as textfile;


INSERT INTO n3_order_overall(order_id, uid, event_time, total_amount, total_cnt,
                             `status`)
VALUES (301001, 101, '2021-10-01 10:00:00', 30000, 3, 1),
       (301002, 102, '2021-10-01 11:00:00', 23900, 2, 1),
       (301003, 103, '2021-10-02 10:00:00', 31000, 2, 1);

INSERT INTO n4_product_info(product_id, shop_id, tag, in_price, quantity, release_time)
VALUES (8001, 901, '家电', 6000, 100, '2020-01-01 10:00:00'),
       (8002, 902, '家电', 12000, 50, '2020-01-01 10:00:00'),
       (8003, 901, '3C数码', 12000, 50, '2020-01-01 10:00:00');

INSERT INTO n5_order_detail(order_id, product_id, price, cnt)
VALUES (301001, 8001, 8500, 2),
       (301001, 8002, 15000, 1),
       (301002, 8001, 8500, 1),
       (301002, 8002, 16000, 1),
       (301003, 8002, 14000, 1),
       (301003, 8003, 18000, 1);

-- 订单总表
select *
from n3_order_overall;

-- 商品信息表
select *
from n4_product_info;

-- 订单明细表
select *
from n5_order_detail;

with temp_table as (
    select n3.order_id,
           total_amount,
           total_cnt,
           n5.product_id,
           shop_id,
           in_price,
           price,
           cnt
    from n3_order_overall n3
             left join n5_order_detail n5 on n3.order_id = n5.order_id
             left join n4_product_info n4 on n4.product_id = n5.product_id
    where shop_id = 901
)
select shop901_gross_margin,
       product_id,
       product_gross_margin
from (
         select product_id, round((1 - max(in_price) / avg(price)) * 100, 1) as product_gross_margin
         from temp_table
         group by product_id
     ) as a,
     (
         select round((1 - sum(in_price * cnt) / sum(`if`(shop_id = 901, price * cnt, 0))) * 100,
                      1) as shop901_gross_margin
         from temp_table
     ) as b
order by product_id asc;

-- 12. 零食类商品中复购率top3高的商品
--      复购率指用户在一段时间内对某商品的重复购买比例，复购率越大，则反映出消费者对品牌的忠诚度就越高，也叫回头率。
--      某商品的复购率 = 近90天内购买它至少两次的人数 ÷ 购买它的总人数
--      近90天指包含最大日期(记为当天)在内的近90天.结果中复购率保留3位小数,并按照复购率倒序、商品id升序排序

drop table if exists n6_product_info;

drop table if exists n7_order_overall;

drop table if exists n8_order_detail;

create table if not exists n7_order_overall
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

create table if not exists n6_product_info
(
    product_id   int comment '商品id',
    shop_id      int comment '店铺id',
    tag          string comment '商品类别标签',
    in_price     decimal comment '进货价格',
    quantity     int comment '进货数量',
    release_time timestamp comment '上架时间'
) comment '商品信息表'
    row format delimited fields terminated by ','
    stored as textfile;

create table if not exists n8_order_detail
(
    order_id   int comment '订单号',
    product_id int comment '商品id',
    price      decimal comment '商品单价',
    cnt        int comment '下单数量'
) comment '订单明细表'
    row format delimited fields terminated by ','
    stored as textfile;

INSERT INTO n6_product_info(product_id, shop_id, tag, in_price, quantity, release_time)
VALUES (8001, 901, '零食', 60, 1000, '2020-01-01 10:00:00'),
       (8002, 901, '零食', 140, 500, '2020-01-01 10:00:00'),
       (8003, 901, '零食', 160, 500, '2020-01-01 10:00:00');

INSERT INTO n7_order_overall(order_id, uid, event_time, total_amount, total_cnt,
                             `status`)
VALUES (301001, 101, '2021-09-30 10:00:00', 140, 1, 1),
       (301002, 102, '2021-10-01 11:00:00', 235, 2, 1),
       (301011, 102, '2021-10-31 11:00:00', 250, 2, 1),
       (301003, 101, '2021-11-02 10:00:00', 300, 2, 1),
       (301013, 105, '2021-11-02 10:00:00', 300, 2, 1),
       (301005, 104, '2021-11-03 10:00:00', 170, 1, 1);

INSERT INTO n8_order_detail(order_id, product_id, price, cnt)
VALUES (301001, 8002, 150, 1),
       (301011, 8003, 200, 1),
       (301011, 8001, 80, 1),
       (301002, 8001, 85, 1),
       (301002, 8003, 180, 1),
       (301003, 8002, 140, 1),
       (301003, 8003, 180, 1),
       (301013, 8002, 140, 2),
       (301005, 8003, 180, 1);

-- 商品信息表
select *
from n6_product_info;

-- 订单总表
select *
from n7_order_overall;

-- 订单明细表
select *
from n8_order_detail;

with temp_table as (
    select n8.product_id,
           date_format(event_time, 'yyyy-MM-dd') as event_time,
           uid
    from n7_order_overall n7
             left join n8_order_detail n8
                       on n7.order_id = n8.order_id
             left join n6_product_info n6
                       on n6.product_id = n8.product_id
    where date_format(event_time, 'yyyy-MM-dd') >=
          date_sub((select date_format(max(event_time), 'yyyy-MM-dd') from n7_order_overall), 89)
      and tag = '零食'
)
select temp_1.product_id,
       round(repurchase_cnt / purchase_cnt, 3) as repurchase_rate
from (select product_id,
             count(distinct uid) as purchase_cnt
      from temp_table
      group by product_id) as temp_1
         left join (
    select product_id,
           count(uid) as repurchase_cnt
    from (select product_id, uid
          from temp_table
          group by product_id, uid
          having count(distinct event_time) >= 2) as table_temp
    group by product_id
) as temp_2
                   on temp_1.product_id = temp_2.product_id
order by repurchase_rate desc, product_id desc;

-- 13. 2021年国庆在北京接单3次及以上的司机
--      请统计2021年国庆7天期间在北京市接单至少3次的司机的平均接单数和平均兼职收入（暂不考虑平台佣金，直接计算完成的订单费用总额）结果保留三位小数
--
--      用户提交打车请求后，用户打车记录表生成一条打车记录 -> order_id订单号为null
--      司机接单后，打车订单表生成一条订单 -> 填充order_time接单时间及其左边的字段
--          start_time -> 开始计费的上车时间及其右边的字段全部为null
--          order_id订单号 & order_time接单时间(打车结束时间) -> 写入打车记录表
--      若一直无司机接单，超时或者中途用户主动取消打车 -> 则记录end_time打车结束时间

--      乘客上车前 -> 乘客或司机点击取消订单 -> 将打车订单表对应订单的 finish_time 订单完成时间 -> 填充为取消时间，其余字段设为null

--      司机接上乘客时 -> 填充订单表中 start_time 开始计费的上车时间

--      订单完成时 -> 填充订单完成时间、里程数、费用 -> 评分为null -> 在用户给司机打1~5星评价后填充

drop table if exists p1_get_car_record;

drop table if exists p2_get_car_order;

create table if not exists p1_get_car_record
(
    uid        int comment '用户id',
    city       string comment '城市',
    event_time timestamp comment '打车时间',
    end_time   timestamp comment '打车结束时间',
    order_id   timestamp comment '订单号'
) comment '用户打车记录表'
    row format delimited fields terminated by ','
    stored as textfile;

create table if not exists p2_get_car_order
(
    order_id    int comment '订单号id',
    uid         int comment '用户id',
    driver_id   int comment '司机id',
    order_time  timestamp comment '接单时间',
    start_time  timestamp comment '开始计费的上车时间',
    finish_time timestamp comment '订单结束时间',
    mileage     double comment '行驶里程数',
    fare        double comment '费用',
    grade       tinyint comment '评分'
) comment '打车订单表'
    row format delimited fields terminated by ','
    stored as textfile;

INSERT INTO p1_get_car_record(uid, city, event_time, end_time, order_id)
VALUES (101, '北京', '2021-10-01 07:00:00', '2021-10-01 07:02:00', null),
       (102, '北京', '2021-10-01 09:00:30', '2021-10-01 09:01:00', 9001),
       (101, '北京', '2021-10-01 08:28:10', '2021-10-01 08:30:00', 9002),
       (103, '北京', '2021-10-02 07:59:00', '2021-10-02 08:01:00', 9003),
       (104, '北京', '2021-10-03 07:59:20', '2021-10-03 08:01:00', 9004),
       (105, '北京', '2021-10-01 08:00:00', '2021-10-01 08:02:10', 9005),
       (106, '北京', '2021-10-01 17:58:00', '2021-10-01 18:01:00', 9006),
       (107, '北京', '2021-10-02 11:00:00', '2021-10-02 11:01:00', 9007),
       (108, '北京', '2021-10-02 21:00:00', '2021-10-02 21:01:00', 9008);

INSERT INTO p2_get_car_order(order_id, uid, driver_id, order_time, start_time,
                             finish_time, mileage, fare, grade)
VALUES ( 9002, 101, 201, '2021-10-01 08:30:00', null, '2021-10-01 08:31:00', null, null
       , null),
       (9001, 102, 202, '2021-10-01 09:01:00', '2021-10-01 09:06:00', '2021-10-01 09:
31:00', 10.0, 41.5, 5),
       (9003, 103, 202, '2021-10-02 08:01:00', '2021-10-02 08:15:00', '2021-10-02 08:
31:00', 11.0, 41.5, 4),
       (9004, 104, 202, '2021-10-03 08:01:00', '2021-10-03 08:13:00', '2021-10-03 08:
31:00', 7.5, 22, 4),
       (9005, 105, 203, '2021-10-01 08:02:10', '2021-10-01 08:18:00', '2021-10-01 08:
31:00', 15.0, 44, 5),
       (9006, 106, 203, '2021-10-01 18:01:00', '2021-10-01 18:09:00', '2021-10-01 18:
31:00', 8.0, 25, 5),
       (9007, 107, 203, '2021-10-02 11:01:00', '2021-10-02 11:07:00', '2021-10-02 11:
31:00', 9.9, 30, 5),
       (9008, 108, 203, '2021-10-02 21:01:00', '2021-10-02 21:10:00', '2021-10-02 21:
31:00', 13.2, 38, 4);

-- 用户打车记录表
select *
from p1_get_car_record;

-- 打车订单表
select *
from p2_get_car_order;

with temp_table as (
    select driver_id,
           date_format(order_time, 'yyyy-MM-dd') as order_time,
           fare
    from p2_get_car_order p2
    where date_format(order_time, 'yyyy-MM-dd') between '2021-10-01' and '2021-10-07'
      and driver_id in (
        select driver_id
        from p2_get_car_order
        group by driver_id
        having count(*) >= 3
    )
)
select round(avg(orders_cnt), 3) as avg_orders_cnt,
       round(avg(fare_total), 3) as avg_fare_total
from (
         select driver_id,
                count(*)                             as orders_cnt,
                sum(`if`(fare is not null, fare, 0)) as fare_total
         from temp_table
         group by driver_id
     ) as table_temp;

-- 14. 有取消订单记录的司机平均评分
--      请找到2021年10月有过取消订单记录的司机，计算他们每人全部已完成的有评分订单的平均评分以及总体平均评分，保留一位小数。先按照dricer_id升序输出，再输出总体情况。

drop table if exists p4_get_car_order;

drop table if exists p3_get_car_record;

create table if not exists p4_get_car_order
(
    order_id    int comment '订单号',
    uid         int comment '用户id',
    driver_id   int comment '司机id',
    order_time  timestamp comment '接单时间',
    start_time  timestamp comment '开始计费的上车时间',
    finish_time timestamp comment '订单结束时间',
    mileage     double comment '行驶里程数',
    fare        double comment '费用',
    grade       tinyint comment '评分'
) comment '打车订单表'
    row format delimited fields terminated by ','
    stored as textfile;

create table if not exists p3_get_car_record
(
    uid        int comment '用户id',
    city       string comment '城市',
    event_time timestamp comment '打车时间',
    end_time   timestamp comment '打车结束时间',
    order_id   int comment '订单号'
) comment '用户打车记录表'
    row format delimited fields terminated by ','
    stored as textfile;

INSERT INTO p3_get_car_record(uid, city, event_time, end_time, order_id)
VALUES (101, '北京', '2021-10-01 07:00:00', '2021-10-01 07:02:00', null),
       (102, '北京', '2021-10-01 09:00:30', '2021-10-01 09:01:00', 9001),
       (101, '北京', '2021-10-01 08:28:10', '2021-10-01 08:30:00', 9002),
       (103, '北京', '2021-10-02 07:59:00', '2021-10-02 08:01:00', 9003),
       (104, '北京', '2021-10-03 07:59:20', '2021-10-03 08:01:00', 9004),
       (105, '北京', '2021-10-01 08:00:00', '2021-10-01 08:02:10', 9005),
       (106, '北京', '2021-10-01 17:58:00', '2021-10-01 18:01:00', 9006),
       (107, '北京', '2021-10-02 11:00:00', '2021-10-02 11:01:00', 9007),
       (108, '北京', '2021-10-02 21:00:00', '2021-10-02 21:01:00', 9008),
       (109, '北京', '2021-10-08 18:00:00', '2021-10-08 18:01:00', 9009);

INSERT INTO p4_get_car_order(order_id, uid, driver_id, order_time, start_time,
                             finish_time, mileage, fare, grade)
VALUES ( 9002, 101, 202, '2021-10-01 08:30:00', null, '2021-10-01 08:31:00', null, null
       , null),
       (9001, 102, 202, '2021-10-01 09:01:00', '2021-10-01 09:06:00', '2021-10-01 09:
31:00', 10.0, 41.5, 5),
       (9003, 103, 202, '2021-10-02 08:01:00', '2021-10-02 08:15:00', '2021-10-02 08:
31:00', 11.0, 41.5, 4),
       (9004, 104, 202, '2021-10-03 08:01:00', '2021-10-03 08:13:00', '2021-10-03 08:
31:00', 7.5, 22, 4),
       ( 9005, 105, 203, '2021-10-01 08:02:10', null, '2021-10-01 08:31:00', null, null
       , null),
       (9006, 106, 203, '2021-10-01 18:01:00', '2021-10-01 18:09:00', '2021-10-01 18:
31:00', 8.0, 25.5, 5),
       (9007, 107, 203, '2021-10-02 11:01:00', '2021-10-02 11:07:00', '2021-10-02 11:
31:00', 9.9, 30, 5),
       (9008, 108, 203, '2021-10-02 21:01:00', '2021-10-02 21:10:00', '2021-10-02 21:
31:00', 13.2, 38, 4),
       (9009, 109, 203, '2021-10-08 18:01:00', '2021-10-08 18:11:50', '2021-10-08 18:
51:00', 13, 40, 5);

-- 用户打车记录表
select *
from p3_get_car_record;

-- 打车订单表
select *
from p4_get_car_order;

with temp_table as (
    select driver_id,
           avg(grade) as avg_grade
    from (
             select driver_id,
                    grade
             from p4_get_car_order
             where driver_id in (
                 select driver_id
                 from p4_get_car_order
                 where start_time is null
             )
               and grade is not null
               and date_format(order_time, 'yyyy-MM') = '2021-10'
         ) as table_temp
    group by driver_id
)
select driver_id,
       round(avg_grade, 1) as avg_grade,
       overall_avg_score
from temp_table,
     (select round(avg(avg_grade), 1) as overall_avg_score from temp_table) as table_temp
order by driver_id asc;

-- 15. 每个城市中评分最高的司机信息
--      请统计每个城市中评分最高的司机平均评分、日均接单量和日均行驶里程数
--      有多个司机评分并列最高时，都输出
--      平均评分和日均接单量保留一位小数
--      日均行驶里程数保留3位小数，按日均接单数升序排序

drop table if exists p5_get_car_record;

drop table if exists p6_get_car_order;

create table if not exists p5_get_car_record
(
    uid        int comment '用户id',
    city       string comment '城市',
    event_time timestamp comment '打车时间',
    end_time   timestamp comment '打车结束时间',
    order_id   int comment '订单号'
) comment '用户打车记录表'
    row format delimited fields terminated by ','
    stored as textfile;

create table if not exists p6_get_car_order
(
    order_id    int comment '订单号',
    uid         int comment '用户id',
    driver_id   int comment '司机id',
    order_time  timestamp comment '接单时间',
    start_time  timestamp comment '开始计费的上车时间',
    finish_time timestamp comment '订单结束时间',
    mileage     double comment '行驶里程数',
    fare        double comment '费用',
    grade       tinyint comment '评分'
) comment '打车订单表'
    row format delimited fields terminated by ','
    stored as textfile;

INSERT INTO p5_get_car_record(uid, city, event_time, end_time, order_id)
VALUES (101, '北京', '2021-10-01 07:00:00', '2021-10-01 07:02:00', null),
       (102, '北京', '2021-10-01 09:00:30', '2021-10-01 09:01:00', 9001),
       (101, '北京', '2021-10-01 08:28:10', '2021-10-01 08:30:00', 9002),
       (103, '北京', '2021-10-02 07:59:00', '2021-10-02 08:01:00', 9003),
       (104, '北京', '2021-10-03 07:59:20', '2021-10-03 08:01:00', 9004),
       (105, '北京', '2021-10-01 08:00:00', '2021-10-01 08:02:10', 9005),
       (106, '北京', '2021-10-01 17:58:00', '2021-10-01 18:01:00', 9006),
       (107, '北京', '2021-10-02 11:00:00', '2021-10-02 11:01:00', 9007),
       (108, '北京', '2021-10-02 21:00:00', '2021-10-02 21:01:00', 9008),
       (109, '北京', '2021-10-08 18:00:00', '2021-10-08 18:01:00', 9009);

INSERT INTO p6_get_car_order(order_id, uid, driver_id, order_time, start_time,
                             finish_time, mileage, fare, grade)
VALUES ( 9002, 101, 202, '2021-10-01 08:30:00', null, '2021-10-01 08:31:00', null, null
       , null),
       (9001, 102, 202, '2021-10-01 09:01:00', '2021-10-01 09:06:00', '2021-10-01 09:
31:00', 10.0, 41.5, 5),
       (9003, 103, 202, '2021-10-02 08:01:00', '2021-10-02 08:15:00', '2021-10-02 08:
31:00', 11.0, 41.5, 4),
       (9004, 104, 202, '2021-10-03 08:01:00', '2021-10-03 08:13:00', '2021-10-03 08:
31:00', 7.5, 22, 4),
       ( 9005, 105, 203, '2021-10-01 08:02:10', null, '2021-10-01 08:31:00', null, null
       , null),
       (9006, 106, 203, '2021-10-01 18:01:00', '2021-10-01 18:09:00', '2021-10-01 18:
31:00', 8.0, 25.5, 5),
       (9007, 107, 203, '2021-10-02 11:01:00', '2021-10-02 11:07:00', '2021-10-02 11:
31:00', 9.9, 30, 5),
       (9008, 108, 203, '2021-10-02 21:01:00', '2021-10-02 21:10:00', '2021-10-02 21:
31:00', 13.2, 38, 4),
       (9009, 109, 203, '2021-10-08 18:01:00', '2021-10-08 18:11:50', '2021-10-08 18:
51:00', 13, 40, 5);

-- 用户打车记录表
select *
from p5_get_car_record;

-- 打车订单表
select *
from p6_get_car_order;

-- 注意这里的平均评分，计算的是有评分的订单的平均评分，不包含没有评分的订单

with temp_table as (
    select driver_id,
           city,
           date_format(order_time, 'yyyy-MM-dd') as day,
           mileage,
           grade
    from p6_get_car_order p6
             left join p5_get_car_record p5
                       on p6.order_id = p5.order_id
)
select city,
       driver_id,
       round(avg_score, 1)       as avg_score,
       round(avg_day_order, 1)   as avg_day_order,
       round(avg_day_mileage, 3) as avg_day_mileage
from (
         select temp_1.city,
                temp_1.driver_id,
                avg_score,
                rank() over (partition by temp_1.city order by avg_score desc) ranking,
                avg_day_mileage,
                avg_day_order
         from (
                  select city,
                         driver_id,
                         avg(day_order_cnt)     as avg_day_order,
                         avg(day_mileage_total) as avg_day_mileage
                  from (
                           select city,
                                  driver_id,
                                  day,
                                  count(*)                               as day_order_cnt,
                                  sum(`if`(mileage is null, 0, mileage)) as day_mileage_total
                           from temp_table
                           group by city, driver_id, day
                       ) as table_temp
                  group by city, driver_id
              ) as temp_1
                  left join (
             select city,
                    driver_id,
                    avg(grade) as avg_score
             from temp_table
             where grade is not null
             group by city, driver_id
         ) as temp_2
                            on temp_1.city = temp_2.city and temp_1.driver_id = temp_2.driver_id
     ) as table_temp
where ranking = 1
order by avg_day_order asc;

-- 16. 国庆期间近7日日均取消订单量
--      请统计国庆头三天里，每天的近7日日均订单完成量和日均订单取消量，按日期升序排序。结果保留两位小数。

drop table if exists p7_get_car_record;

drop table if exists p8_get_car_order;

create table if not exists p7_get_car_record
(
    uid        int comment '用户id',
    city       string comment '城市',
    event_time timestamp comment '打车时间',
    end_time   timestamp comment '打车结束时间',
    order_id   int comment '订单号'
) comment '用户打车记录表'
    row format delimited fields terminated by ','
    stored as textfile;

create table if not exists p8_get_car_order
(
    order_id    int comment '订单号',
    uid         int comment '用户id',
    driver_id   int comment '司机id',
    order_time  timestamp comment '接单时间',
    start_time  timestamp comment '开始计费的上车时间',
    finish_time timestamp comment '订单结束时间',
    mileage     double comment '行驶里程数',
    fare        double comment '费用',
    grade       tinyint comment '评分'
) comment '打车订单表'
    row format delimited fields terminated by ','
    stored as textfile;

INSERT INTO p7_get_car_record(uid, city, event_time, end_time, order_id)
VALUES (101, '北京', '2021-09-25 08:28:10', '2021-09-25 08:30:00', 9011),
       (102, '北京', '2021-09-25 09:00:30', '2021-09-25 09:01:00', 9012),
       (103, '北京', '2021-09-26 07:59:00', '2021-09-26 08:01:00', 9013),
       (104, '北京', '2021-09-26 07:59:00', '2021-09-26 08:01:00', 9023),
       (104, '北京', '2021-09-27 07:59:20', '2021-09-27 08:01:00', 9014),
       (105, '北京', '2021-09-28 08:00:00', '2021-09-28 08:02:10', 9015),
       (106, '北京', '2021-09-29 17:58:00', '2021-09-29 18:01:00', 9016),
       (107, '北京', '2021-09-30 11:00:00', '2021-09-30 11:01:00', 9017),
       (108, '北京', '2021-09-30 21:00:00', '2021-09-30 21:01:00', 9018),
       (102, '北京', '2021-10-01 09:00:30', '2021-10-01 09:01:00', 9002),
       (106, '北京', '2021-10-01 17:58:00', '2021-10-01 18:01:00', 9006),
       (101, '北京', '2021-10-02 08:28:10', '2021-10-02 08:30:00', 9001),
       (107, '北京', '2021-10-02 11:00:00', '2021-10-02 11:01:00', 9007),
       (108, '北京', '2021-10-02 21:00:00', '2021-10-02 21:01:00', 9008),
       (103, '北京', '2021-10-02 07:59:00', '2021-10-02 08:01:00', 9003),
       (104, '北京', '2021-10-03 07:59:20', '2021-10-03 08:01:00', 9004),
       (109, '北京', '2021-10-03 18:00:00', '2021-10-03 18:01:00', 9009);

INSERT INTO p8_get_car_order(order_id, uid, driver_id, order_time, start_time,
                             finish_time, mileage, fare, grade)
VALUES (9011, 101, 211, '2021-09-25 08:30:00', '2021-09-25 08:31:00', '2021-09-25 08:
54:00', 10, 35, 5),
       (9012, 102, 211, '2021-09-25 09:01:00', '2021-09-25 09:01:50', '2021-09-25 09:
28:00', 11, 32, 5),
       (9013, 103, 212, '2021-09-26 08:01:00', '2021-09-26 08:03:00', '2021-09-26 08:
27:00', 12, 31, 4),
       ( 9023, 104, 213, '2021-09-26 08:01:00', null, '2021-09-26 08:27:00', null, null
       , null),
       (9014, 104, 212, '2021-09-27 08:01:00', '2021-09-27 08:04:00', '2021-09-27 08:
21:00', 11, 31, 5),
       (9015, 105, 212, '2021-09-28 08:02:10', '2021-09-28 08:04:10', '2021-09-28 08:
25:10', 12, 31, 4),
       (9016, 106, 213, '2021-09-29 18:01:00', '2021-09-29 18:02:10', '2021-09-29 18:
23:00', 11, 39, 4),
       (9017, 107, 213, '2021-09-30 11:01:00', '2021-09-30 11:01:40', '2021-09-30 11:
31:00', 11, 38, 5),
       (9018, 108, 214, '2021-09-30 21:01:00', '2021-09-30 21:02:50', '2021-09-30 21:
21:00', 14, 38, 5),
       (9002, 102, 202, '2021-10-01 09:01:00', '2021-10-01 09:06:00', '2021-10-01 09:
31:00', 10.0, 41.5, 5),
       (9006, 106, 203, '2021-10-01 18:01:00', '2021-10-01 18:09:00', '2021-10-01 18:
31:00', 8.0, 25.5, 4),
       ( 9001, 101, 202, '2021-10-02 08:30:00', null, '2021-10-02 08:31:00', null, null
       , null),
       (9007, 107, 203, '2021-10-02 11:01:00', '2021-10-02 11:07:00', '2021-10-02 11:
31:00', 9.9, 30, 5),
       (9008, 108, 204, '2021-10-02 21:01:00', '2021-10-02 21:10:00', '2021-10-02 21:
31:00', 13.2, 38, 4),
       (9003, 103, 202, '2021-10-02 08:01:00', '2021-10-02 08:15:00', '2021-10-02 08:
31:00', 11.0, 41.5, 4),
       (9004, 104, 202, '2021-10-03 08:01:00', '2021-10-03 08:13:00', '2021-10-03 08:
31:00', 7.5, 22, 4),
       ( 9009, 109, 204, '2021-10-03 18:01:00', null, '2021-10-03 18:51:00', null, null
       , null);

-- 用户打车记录表
select *
from p7_get_car_record;

-- 打车订单表
select *
from p8_get_car_order;

select *
from p8_get_car_order;

set hive.execution.engine = mr;

set hive.execution.engine = spark;

with temp_table as (
    select date_format(order_time, 'yyyy-MM-dd')    as order_time,
           count(`if`(start_time is null, null, 1)) as orders_cnt,
           count(`if`(start_time is null, 1, null)) as cancel_orders_cnt
    from p8_get_car_order
    group by date_format(order_time, 'yyyy-MM-dd')
)
select *
from (
         select order_time,
                round(avg(orders_cnt) over (order by order_time asc rows between 6 preceding and current row ),
                      2) as avg_orders_cnt,
                round(avg(cancel_orders_cnt) over (order by order_time asc rows between 6 preceding and current row ),
                      2) as avg_cancel_orders_cnt
         from temp_table t
     ) as table_temp
where order_time between '2021-10-01' and '2021-10-03'
order by order_time asc;

-- 17. 工作日各时段叫车量、等待接单时间和调度时间
--      统计周一到周五各时段的叫车量、平均等待接单时间(一位小数)和平均调度时间(一位小数)。
--      event_time -> 为打车时间段划分依据
--      平均调度时间仅计算完成了的订单 -> 结果按叫车量升序排序

--      早高峰 [07:00:00 , 09:00:00)、工作时间 [09:00:00 , 17:00:00）、晚高峰 [17:00:00 ,20:00:00）、休息时间 [20:00:00 , 07:00:00）
--      开始打车到司机接单为等待接单时间 -> 司机接单到上车为调度时间

drop table if exists p9_get_car_record;

drop table if exists p10_get_car_order;

create table if not exists p9_get_car_record
(
    uid        int comment '用户id',
    city       string comment '城市',
    event_time timestamp comment '打车时间',
    end_time   timestamp comment '打车结束时间',
    order_id   int comment '订单号'
) comment '用户打车记录表'
    row format delimited fields terminated by ','
    stored as textfile;

create table if not exists p10_get_car_order
(
    order_id    int comment '订单号',
    uid         int comment '用户id',
    driver_id   int comment '司机id',
    order_time  timestamp comment '接单时间',
    start_time  timestamp comment '开始计费的上车时间',
    finish_time timestamp comment '订单结束时间',
    mileage     double comment '行驶里程数',
    fare        double comment '费用',
    grade       tinyint comment '评分'
) comment '打车订单表'
    row format delimited fields terminated by ','
    stored as textfile;

INSERT INTO p9_get_car_record(uid, city, event_time, end_time, order_id)
VALUES (107, '北京', '2021-09-20 11:00:00', '2021-09-20 11:00:30', 9017),
       (108, '北京', '2021-09-20 21:00:00', '2021-09-20 21:00:40', 9008),
       (108, '北京', '2021-09-20 18:59:30', '2021-09-20 19:01:00', 9018),
       (102, '北京', '2021-09-21 08:59:00', '2021-09-21 09:01:00', 9002),
       (106, '北京', '2021-09-21 17:58:00', '2021-09-21 18:01:00', 9006),
       (103, '北京', '2021-09-22 07:58:00', '2021-09-22 08:01:00', 9003),
       (104, '北京', '2021-09-23 07:59:00', '2021-09-23 08:01:00', 9004),
       (103, '北京', '2021-09-24 19:59:20', '2021-09-24 20:01:00', 9019),
       (101, '北京', '2021-09-24 08:28:10', '2021-09-24 08:30:00', 9011);

INSERT INTO p10_get_car_order(order_id, uid, driver_id, order_time, start_time,
                              finish_time, mileage, fare, grade)
VALUES (9017, 107, 213, '2021-09-20 11:00:30', '2021-09-20 11:02:10', '2021-09-20 11:
31:00', 11, 38, 5),
       (9008, 108, 204, '2021-09-20 21:00:40', '2021-09-20 21:03:00', '2021-09-20 21:
31:00', 13.2, 38, 4),
       (9018, 108, 214, '2021-09-20 19:01:00', '2021-09-20 19:04:50', '2021-09-20 19:
21:00', 14, 38, 5),
       (9002, 102, 202, '2021-09-21 09:01:00', '2021-09-21 09:06:00', '2021-09-21 09:
31:00', 10.0, 41.5, 5),
       (9006, 106, 203, '2021-09-21 18:01:00', '2021-09-21 18:09:00', '2021-09-21 18:
31:00', 8.0, 25.5, 4),
       (9007, 107, 203, '2021-09-22 11:01:00', '2021-09-22 11:07:00', '2021-09-22 11:
31:00', 9.9, 30, 5),
       (9003, 103, 202, '2021-09-22 08:01:00', '2021-09-22 08:15:00', '2021-09-22 08:
31:00', 11.0, 41.5, 4),
       (9004, 104, 202, '2021-09-23 08:01:00', '2021-09-23 08:13:00', '2021-09-23 08:
31:00', 7.5, 22, 4),
       (9005, 105, 202, '2021-09-23 10:01:00', '2021-09-23 10:13:00', '2021-09-23 10:
31:00', 9, 29, 5),
       (9019, 103, 202, '2021-09-24 20:01:00', '2021-09-24 20:11:00', '2021-09-24 20:
51:00', 10, 39, 4),
       (9011, 101, 211, '2021-09-24 08:30:00', '2021-09-24 08:31:00', '2021-09-24 08:
54:00', 10, 35, 5);

-- 用户打车记录表
select *
from p9_get_car_record;

-- 打车订单表
select *
from p10_get_car_order;

with temp_table as (
    select case
               when hour(event_time) >= 7 and hour(event_time) < 9 then '早高峰'
               when hour(event_time) >= 9 and hour(event_time) < 17 then '工作时间'
               when hour(event_time) >= 17 and hour(event_time) < 20 then '晚高峰'
               else '休息时间'
               end                                                 as time_segment,
           unix_timestamp(order_time) - unix_timestamp(event_time) as waiting_duration,
           unix_timestamp(start_time) - unix_timestamp(order_time) as schedule_duration
    from p9_get_car_record p9
             join p10_get_car_order p10
                  on p9.order_id = p10.order_id
    where `dayofweek`(date_format(event_time, 'yyyy-MM-dd')) <> 6
      and `dayofweek`(date_format(event_time, 'yyyy-MM-dd')) <> 0
)
select time_segment,
       count(*)                              as call_cnt,
       round(avg(waiting_duration) / 60, 1)  as avg_waiting_duration,
       round(avg(schedule_duration) / 60, 1) as avg_schedule_duration
from temp_table
group by time_segment
order by call_cnt asc;

-- 18. 各城市最大同时等车人数
--      请统计各个城市在2021年10月期间，单日中最大的同时等车人数。
--      等车 -> 从开始打车起，直到取消打车、取消等待或上车前的这段时间 -> 里面的用户状态
--      如果同一时刻有人停止等车，有人开始等车，等车人数记作先增加后减少
--      结果按照各城市最大等车人数升序排序，相同时按城市升序排序

drop table if exists p12_get_car_order;

drop table if exists p11_get_car_record;

create table if not exists p12_get_car_order
(
    order_id    int comment '订单号',
    uid         int comment '用户id',
    driver_id   int comment '司机id',
    order_time  timestamp comment '接单时间',
    start_time  timestamp comment '开始计费的上车时间',
    finish_time timestamp comment '订单结束时间',
    mileage     double comment '行驶里程数',
    fare        double comment '费用',
    grade       tinyint comment '评分'
) comment '打车订单表'
    row format delimited fields terminated by ','
    stored as textfile;

create table if not exists p11_get_car_record
(
    uid        int comment '用户id',
    city       string comment '城市',
    event_time timestamp comment '打车时间',
    end_time   timestamp comment '打车结束时间',
    order_id   int comment '订单号'
) comment '用户打车记录表'
    row format delimited fields terminated by ','
    stored as textfile;

INSERT INTO p11_get_car_record(uid, city, event_time, end_time, order_id)
VALUES (108, '北京', '2021-10-20 08:00:00', '2021-10-20 08:00:40', 9008),
       (108, '北京', '2021-10-20 08:00:10', '2021-10-20 08:00:45', 9018),
       (102, '北京', '2021-10-20 08:00:30', '2021-10-20 08:00:50', 9002),
       (106, '北京', '2021-10-20 08:05:41', '2021-10-20 08:06:00', 9006),
       (103, '北京', '2021-10-20 08:05:50', '2021-10-20 08:07:10', 9003),
       (104, '北京', '2021-10-20 08:01:01', '2021-10-20 08:01:20', 9004),
       (103, '北京', '2021-10-20 08:01:15', '2021-10-20 08:01:30', 9019),
       (101, '北京', '2021-10-20 08:28:10', '2021-10-20 08:30:00', 9011);

INSERT INTO p12_get_car_order(order_id, uid, driver_id, order_time, start_time,
                              finish_time, mileage, fare, grade)
VALUES (9008, 108, 204, '2021-10-20 08:00:40', '2021-10-20 08:03:00', '2021-10-20 08:
31:00', 13.2, 38, 4),
       (9018, 108, 214, '2021-10-20 08:00:45', '2021-10-20 08:04:50', '2021-10-20 08:
21:00', 14, 38, 5),
       (9002, 102, 202, '2021-10-20 08:00:50', '2021-10-20 08:06:00', '2021-10-20 08:
31:00', 10.0, 41.5, 5),
       (9006, 106, 203, '2021-10-20 08:06:00', '2021-10-20 08:09:00', '2021-10-20 08:
31:00', 8.0, 25.5, 4),
       (9003, 103, 202, '2021-10-20 08:07:10', '2021-10-20 08:15:00', '2021-10-20 08:
31:00', 11.0, 41.5, 4),
       (9004, 104, 202, '2021-10-20 08:01:20', '2021-10-20 08:13:00', '2021-10-20 08:
31:00', 7.5, 22, 4),
       (9019, 103, 202, '2021-10-20 08:01:30', '2021-10-20 08:11:00', '2021-10-20 08:
51:00', 10, 39, 4),
       (9011, 101, 211, '2021-10-20 08:30:00', '2021-10-20 08:31:00', '2021-10-20 08:
54:00', 10, 35, 5);

-- 打车订单表
select *
from p12_get_car_order;

-- 用户打车记录表
select *
from p11_get_car_record;

with temp_table as (
    select city,
           event_time as ts,
           1          as flag
    from p11_get_car_record p11
             left join p12_get_car_order p12
                       on p11.order_id = p12.order_id
    union all
    select city,
           start_time as ts,
           -1         as flag
    from p11_get_car_record p11
             left join p12_get_car_order p12
                       on p11.order_id = p12.order_id
)
select city,
       max(multi_users) as max_cnt
from (
         select city,
                ts,
                sum(flag)
                    over (partition by city, date_format(ts, 'yyyy-MM-dd') order by ts asc rows between unbounded preceding and current row ) as multi_users
         from temp_table
         where date_format(ts, 'yyyy-MM') = '2021-10'
     ) as table_temp
group by city
order by max_cnt asc, city asc;

-- 19. 某宝店铺的spu数量
--      统计每款的spu(货号)数量，按spu数量降序

drop table if exists q1_product_tb;

create table if not exists q1_product_tb
(
    item_id   string comment '',
    style_id  string comment '',
    tag_price int comment '',
    inventory int comment ''
) comment '产品情况表'
    row format delimited fields terminated by ','
    stored as textfile;

INSERT INTO q1_product_tb
VALUES ('A001', 'A', 100, 20);
INSERT INTO q1_product_tb
VALUES ('A002', 'A', 120, 30);
INSERT INTO q1_product_tb
VALUES ('A003', 'A', 200, 15);
INSERT INTO q1_product_tb
VALUES ('B001', 'B', 130, 18);
INSERT INTO q1_product_tb
VALUES ('B002', 'B', 150, 22);
INSERT INTO q1_product_tb
VALUES ('B003', 'B', 125, 10);
INSERT INTO q1_product_tb
VALUES ('B004', 'B', 155, 12);
INSERT INTO q1_product_tb
VALUES ('C001', 'C', 260, 25);
INSERT INTO q1_product_tb
VALUES ('C002', 'C', 280, 18);

-- 产品情况表
select *
from q1_product_tb;

select style_id,
       count(item_id) as nums
from q1_product_tb
group by style_id
order by nums desc;

-- 20. 某宝店铺的实际销售额与客单价
--      统计11月份实际总销售额与客单价
--      客单价 -> 人均付费 -> 总收入 / 总用户数 -> 结果保留两位小数

drop table if exists q2_product_tb;

create table if not exists q2_product_tb
(
    sales_date  date comment '',
    user_id     int comment '',
    item_id     string comment '',
    sales_num   int comment '',
    sales_price int comment ''
) comment '销售数据表'
    row format delimited fields terminated by ','
    stored as textfile;

INSERT INTO q2_product_tb
VALUES ('2021-11-1', 1, 'A001', 1, 90);
INSERT INTO q2_product_tb
VALUES ('2021-11-1', 2, 'A002', 2, 220);
INSERT INTO q2_product_tb
VALUES ('2021-11-1', 2, 'B001', 1, 120);
INSERT INTO q2_product_tb
VALUES ('2021-11-2', 3, 'C001', 2, 500);
INSERT INTO q2_product_tb
VALUES ('2021-11-2', 4, 'B001', 1, 120);
INSERT INTO q2_product_tb
VALUES ('2021-11-3', 5, 'C001', 1, 240);
INSERT INTO q2_product_tb
VALUES ('2021-11-3', 6, 'C002', 1, 270);
INSERT INTO q2_product_tb
VALUES ('2021-11-4', 7, 'A003', 1, 180);
INSERT INTO q2_product_tb
VALUES ('2021-11-4', 8, 'B002', 1, 140);
INSERT INTO q2_product_tb
VALUES ('2021-11-4', 9, 'B001', 1, 125);
INSERT INTO q2_product_tb
VALUES ('2021-11-5', 10, 'B003', 1, 120);
INSERT INTO q2_product_tb
VALUES ('2021-11-5', 10, 'B004', 1, 150);
INSERT INTO q2_product_tb
VALUES ('2021-11-5', 10, 'A003', 1, 180);
INSERT INTO q2_product_tb
VALUES ('2021-11-6', 11, 'B003', 1, 120);
INSERT INTO q2_product_tb
VALUES ('2021-11-6', 10, 'B004', 1, 150);

-- 销售数据表
select *
from q2_product_tb;

select sum(sales_price)                                     as sales_total,
       round(sum(sales_price) / count(distinct user_id), 2) as sales_per_user
from q2_product_tb
where date_format(sales_date, 'yyyy-MM') = '2021-11';

-- 21. 某宝店铺折扣率
--      统计折扣率（GMV / 吊牌金额）
--      GMV -> 指成交金额

drop table if exists q3_product_tb;

drop table if exists q4_sales_tb;

create table if not exists q3_product_tb
(
    item_id   string comment '',
    style_id  string comment '',
    tag_price int comment '',
    inventory int comment ''
) comment '产品情况表'
    row format delimited fields terminated by ','
    stored as textfile;

create table if not exists q4_sales_tb
(
    sales_date  date comment '',
    user_id     int comment '',
    item_id     string comment '',
    sales_num   int comment '',
    sales_price int comment ''
) comment '销售数据表'
    row format delimited fields terminated by ','
    stored as textfile;

INSERT INTO q3_product_tb
VALUES ('A001', 'A', 100, 20);
INSERT INTO q3_product_tb
VALUES ('A002', 'A', 120, 30);
INSERT INTO q3_product_tb
VALUES ('A003', 'A', 200, 15);
INSERT INTO q3_product_tb
VALUES ('B001', 'B', 130, 18);
INSERT INTO q3_product_tb
VALUES ('B002', 'B', 150, 22);
INSERT INTO q3_product_tb
VALUES ('B003', 'B', 125, 10);
INSERT INTO q3_product_tb
VALUES ('B004', 'B', 155, 12);
INSERT INTO q3_product_tb
VALUES ('C001', 'C', 260, 25);
INSERT INTO q3_product_tb
VALUES ('C002', 'C', 280, 18);

INSERT INTO q4_sales_tb
VALUES ('2021-11-1', 1, 'A001', 1, 90);
INSERT INTO q4_sales_tb
VALUES ('2021-11-1', 2, 'A002', 2, 220);
INSERT INTO q4_sales_tb
VALUES ('2021-11-1', 2, 'B001', 1, 120);
INSERT INTO q4_sales_tb
VALUES ('2021-11-2', 3, 'C001', 2, 500);
INSERT INTO q4_sales_tb
VALUES ('2021-11-2', 4, 'B001', 1, 120);
INSERT INTO q4_sales_tb
VALUES ('2021-11-3', 5, 'C001', 1, 240);
INSERT INTO q4_sales_tb
VALUES ('2021-11-3', 6, 'C002', 1, 270);
INSERT INTO q4_sales_tb
VALUES ('2021-11-4', 7, 'A003', 1, 180);
INSERT INTO q4_sales_tb
VALUES ('2021-11-4', 8, 'B002', 1, 140);
INSERT INTO q4_sales_tb
VALUES ('2021-11-4', 9, 'B001', 1, 125);
INSERT INTO q4_sales_tb
VALUES ('2021-11-5', 10, 'B003', 1, 120);
INSERT INTO q4_sales_tb
VALUES ('2021-11-5', 10, 'B004', 1, 150);
INSERT INTO q4_sales_tb
VALUES ('2021-11-5', 10, 'A003', 1, 180);
INSERT INTO q4_sales_tb
VALUES ('2021-11-6', 11, 'B003', 1, 120);
INSERT INTO q4_sales_tb
VALUES ('2021-11-6', 10, 'B004', 1, 150);

-- 产品情况表
select *
from q3_product_tb;

-- 销售数据表
select *
from q4_sales_tb;

select round(sum(sales_price) / sum(tag_price * sales_num) * 100, 2) as discount_rate
from q4_sales_tb q4
         left join q3_product_tb q3
                   on q4.item_id = q3.item_id;

-- 22. 某宝店铺动销率与售罄率
--      统计每款的动销率与售罄率 -> 按style_id升序
--      动销率 -> pin_rate -> 有销售的数量 / 在售sku数量
--      售罄率 -> sellThrough_rate -> GMV / 备货值
--      备货值 -> 吊牌价 * 库存数

set hive.execution.engine = mr;

set hive.execution.engine = spark;

drop table if exists q5_product_tb;

drop table if exists q6_sales_tb;

create table if not exists q5_product_tb
(
    item_id   string comment '',
    style_id  string comment '',
    tag_price int comment '',
    inventory int comment ''
) comment '产品情况表'
    row format delimited fields terminated by ','
    stored as textfile;

create table if not exists q6_sales_tb
(
    sales_date  date comment '',
    user_id     int comment '',
    item_id     string comment '',
    sales_num   int comment '',
    sales_price int comment ''
) comment '销售数据报表'
    row format delimited fields terminated by ','
    stored as textfile;

INSERT INTO q5_product_tb
VALUES ('A001', 'A', 100, 20);
INSERT INTO q5_product_tb
VALUES ('A002', 'A', 120, 30);
INSERT INTO q5_product_tb
VALUES ('A003', 'A', 200, 15);
INSERT INTO q5_product_tb
VALUES ('B001', 'B', 130, 18);
INSERT INTO q5_product_tb
VALUES ('B002', 'B', 150, 22);
INSERT INTO q5_product_tb
VALUES ('B003', 'B', 125, 10);
INSERT INTO q5_product_tb
VALUES ('B004', 'B', 155, 12);
INSERT INTO q5_product_tb
VALUES ('C001', 'C', 260, 25);
INSERT INTO q5_product_tb
VALUES ('C002', 'C', 280, 18);

INSERT INTO q6_sales_tb
VALUES ('2021-11-1', 1, 'A001', 1, 90);
INSERT INTO q6_sales_tb
VALUES ('2021-11-1', 2, 'A002', 2, 220);
INSERT INTO q6_sales_tb
VALUES ('2021-11-1', 2, 'B001', 1, 120);
INSERT INTO q6_sales_tb
VALUES ('2021-11-2', 3, 'C001', 2, 500);
INSERT INTO q6_sales_tb
VALUES ('2021-11-2', 4, 'B001', 1, 120);
INSERT INTO q6_sales_tb
VALUES ('2021-11-3', 5, 'C001', 1, 240);
INSERT INTO q6_sales_tb
VALUES ('2021-11-3', 6, 'C002', 1, 270);
INSERT INTO q6_sales_tb
VALUES ('2021-11-4', 7, 'A003', 1, 180);
INSERT INTO q6_sales_tb
VALUES ('2021-11-4', 8, 'B002', 1, 140);
INSERT INTO q6_sales_tb
VALUES ('2021-11-4', 9, 'B001', 1, 125);
INSERT INTO q6_sales_tb
VALUES ('2021-11-5', 10, 'B003', 1, 120);
INSERT INTO q6_sales_tb
VALUES ('2021-11-5', 10, 'B004', 1, 150);
INSERT INTO q6_sales_tb
VALUES ('2021-11-5', 10, 'A003', 1, 180);
INSERT INTO q6_sales_tb
VALUES ('2021-11-6', 11, 'B003', 1, 120);
INSERT INTO q6_sales_tb
VALUES ('2021-11-6', 10, 'B004', 1, 150);

-- 产品情况表
select *
from q5_product_tb;

-- 销售数据表
select *
from q6_sales_tb;

with a as (
    select style_id,
           sum(inventory)             as pre_online_nums,
           sum(tag_price * inventory) as inventory_value
    from q5_product_tb
    group by style_id
)
select style_id,
       round(sum(sales_num) / ((select pre_online_nums from a where a.style_id = q5.style_id) - sum(sales_num)) * 100,
             2) as pin_rate,
       round(sum(sales_price) / (select inventory_value from a where a.style_id = q5.style_id) * 100,
             2) as sellThrough_rate
from q5_product_tb q5
         left join q6_sales_tb q6
                   on q5.item_id = q6.item_id
group by style_id
order by style_id asc;

-- 23. 某宝店铺连续两天及以上购物的用户及其对应的天数
--      请统计连续2天及以上在该店铺购物的用户及其对应的次数
--      若有多个用户，按user_id升序

drop table if exists q7_sales_tb;

create table if not exists q7_sales_tb
(
    sales_date  date comment '',
    user_id     int comment '',
    item_id     string comment '',
    sales_num   int comment '',
    sales_price int comment ''
) comment '销售数据表'
    row format delimited fields terminated by ','
    stored as textfile;

INSERT INTO q7_sales_tb
VALUES ('2021-11-1', 1, 'A001', 1, 90);
INSERT INTO q7_sales_tb
VALUES ('2021-11-1', 2, 'A002', 2, 220);
INSERT INTO q7_sales_tb
VALUES ('2021-11-1', 2, 'B001', 1, 120);
INSERT INTO q7_sales_tb
VALUES ('2021-11-2', 3, 'C001', 2, 500);
INSERT INTO q7_sales_tb
VALUES ('2021-11-2', 4, 'B001', 1, 120);
INSERT INTO q7_sales_tb
VALUES ('2021-11-3', 5, 'C001', 1, 240);
INSERT INTO q7_sales_tb
VALUES ('2021-11-3', 6, 'C002', 1, 270);
INSERT INTO q7_sales_tb
VALUES ('2021-11-4', 7, 'A003', 1, 180);
INSERT INTO q7_sales_tb
VALUES ('2021-11-4', 8, 'B002', 1, 140);
INSERT INTO q7_sales_tb
VALUES ('2021-11-4', 9, 'B001', 1, 125);
INSERT INTO q7_sales_tb
VALUES ('2021-11-5', 10, 'B003', 1, 120);
INSERT INTO q7_sales_tb
VALUES ('2021-11-5', 10, 'B004', 1, 150);
INSERT INTO q7_sales_tb
VALUES ('2021-11-5', 10, 'A003', 1, 180);
INSERT INTO q7_sales_tb
VALUES ('2021-11-6', 11, 'B003', 1, 120);
INSERT INTO q7_sales_tb
VALUES ('2021-11-6', 10, 'B004', 1, 150);

-- 销售数据表
select *
from q7_sales_tb;

select user_id,
       count(date_sub(sales_date, ranking)) as cnt
from (
         select user_id,
                sales_date,
                rank() over (partition by user_id order by sales_date asc) ranking
         from (
                  select sales_date,
                         user_id
                  from q7_sales_tb
                  group by sales_date, user_id
              ) as a
     ) as b
group by user_id
having count(date_sub(sales_date, ranking)) >= 2
order by user_id asc;

-- 24. 牛客直播转换率
--      统计每个科目的转换率 -> sign_rate(%) -> 报名人数 / 浏览人数
--      结果保留两位小数，按course_id升序

drop table if exists r1_course_tb;

drop table if exists r2_behavior_tb;

create table if not exists r1_course_tb
(
    course_id       int comment '',
    course_name     string comment '',
    course_datetime string comment ''
) comment '课程表'
    row format delimited fields terminated by ','
    stored as textfile;

create table if not exists r2_behavior_tb
(
    user_id   int comment '',
    if_vw     int comment '',
    if_fav    int comment '',
    if_sign   int comment '',
    course_id int comment ''
) comment '用户行为表'
    row format delimited fields terminated by ','
    stored as textfile;

INSERT INTO r1_course_tb
VALUES (1, 'Python', '2021-12-1 19:00-21:00');
INSERT INTO r1_course_tb
VALUES (2, 'SQL', '2021-12-2 19:00-21:00');
INSERT INTO r1_course_tb
VALUES (3, 'R', '2021-12-3 19:00-21:00');

INSERT INTO r2_behavior_tb
VALUES (100, 1, 1, 1, 1);
INSERT INTO r2_behavior_tb
VALUES (100, 1, 1, 1, 2);
INSERT INTO r2_behavior_tb
VALUES (100, 1, 1, 1, 3);
INSERT INTO r2_behavior_tb
VALUES (101, 1, 1, 1, 1);
INSERT INTO r2_behavior_tb
VALUES (101, 1, 1, 1, 2);
INSERT INTO r2_behavior_tb
VALUES (101, 1, 0, 0, 3);
INSERT INTO r2_behavior_tb
VALUES (102, 1, 1, 1, 1);
INSERT INTO r2_behavior_tb
VALUES (102, 1, 1, 1, 2);
INSERT INTO r2_behavior_tb
VALUES (102, 1, 1, 1, 3);
INSERT INTO r2_behavior_tb
VALUES (103, 1, 1, 0, 1);
INSERT INTO r2_behavior_tb
VALUES (103, 1, 0, 0, 2);
INSERT INTO r2_behavior_tb
VALUES (103, 1, 0, 0, 3);
INSERT INTO r2_behavior_tb
VALUES (104, 1, 1, 1, 1);
INSERT INTO r2_behavior_tb
VALUES (104, 1, 1, 1, 2);
INSERT INTO r2_behavior_tb
VALUES (104, 1, 1, 0, 3);
INSERT INTO r2_behavior_tb
VALUES (105, 1, 0, 0, 1);
INSERT INTO r2_behavior_tb
VALUES (106, 1, 0, 0, 1);
INSERT INTO r2_behavior_tb
VALUES (107, 1, 0, 0, 1);
INSERT INTO r2_behavior_tb
VALUES (107, 1, 1, 1, 2);
INSERT INTO r2_behavior_tb
VALUES (108, 1, 1, 1, 3);

-- 课程表
select *
from r1_course_tb;

-- 用户行为表
select *
from r2_behavior_tb;

select r2.course_id,
       course_name,
       round(sum(if_sign) / sum(if_vw) * 100, 2) as sign_rate
from r2_behavior_tb r2
         left join r1_course_tb r1
                   on r2.course_id = r1.course_id
group by course_name, r2.course_id
order by r2.course_id asc;

-- 25. 牛客直播开始时各直播间在线人数
--      统计直播开始时(19:00)，各科目的在线人数 -> 按course_id升序

drop table if exists r3_course_tb;

drop table if exists r4_attend_tb;

create table if not exists r3_course_tb
(
    course_id       int comment '',
    course_name     string comment '',
    course_datetime string comment ''
) comment '课程表'
    row format delimited fields terminated by ','
    stored as textfile;

create table if not exists r4_attend_tb
(
    user_id      int comment '',
    course_id    int comment '',
    in_datetime  timestamp comment '',
    out_datetime timestamp comment ''
) comment '上课情况表'
    row format delimited fields terminated by ','
    stored as textfile;

INSERT INTO r3_course_tb
VALUES (1, 'Python', '2021-12-1 19:00-21:00');
INSERT INTO r3_course_tb
VALUES (2, 'SQL', '2021-12-2 19:00-21:00');
INSERT INTO r3_course_tb
VALUES (3, 'R', '2021-12-3 19:00-21:00');

INSERT INTO r4_attend_tb
VALUES (100, 1, '2021-12-1 19:00:00', '2021-12-1 19:28:00');
INSERT INTO r4_attend_tb
VALUES (100, 1, '2021-12-1 19:30:00', '2021-12-1 19:53:00');
INSERT INTO r4_attend_tb
VALUES (101, 1, '2021-12-1 19:00:00', '2021-12-1 20:55:00');
INSERT INTO r4_attend_tb
VALUES (102, 1, '2021-12-1 19:00:00', '2021-12-1 19:05:00');
INSERT INTO r4_attend_tb
VALUES (104, 1, '2021-12-1 19:00:00', '2021-12-1 20:59:00');
INSERT INTO r4_attend_tb
VALUES (101, 2, '2021-12-2 19:05:00', '2021-12-2 20:58:00');
INSERT INTO r4_attend_tb
VALUES (102, 2, '2021-12-2 18:55:00', '2021-12-2 21:00:00');
INSERT INTO r4_attend_tb
VALUES (104, 2, '2021-12-2 18:57:00', '2021-12-2 20:56:00');
INSERT INTO r4_attend_tb
VALUES (107, 2, '2021-12-2 19:10:00', '2021-12-2 19:18:00');
INSERT INTO r4_attend_tb
VALUES (100, 3, '2021-12-3 19:01:00', '2021-12-3 21:00:00');
INSERT INTO r4_attend_tb
VALUES (102, 3, '2021-12-3 18:58:00', '2021-12-3 19:05:00');
INSERT INTO r4_attend_tb
VALUES (108, 3, '2021-12-3 19:01:00', '2021-12-3 19:56:00');

-- 课程表
select *
from r3_course_tb;

-- 上课情况表
select *
from r4_attend_tb;

with a as (
    select r4.course_id,
           course_name,
           date_format(in_datetime, 'HH:mm:ss')  as in_datetime,
           date_format(out_datetime, 'HH:mm:ss') as out_datetime
    from r4_attend_tb r4
             left join r3_course_tb r3
                       on r4.course_id = r3.course_id
),
     b as (
         select course_id,
                course_name,
                in_datetime as date_time,
                1           as flag
         from a
         union all
         select course_id,
                course_name,
                out_datetime as date_time,
                -1           as flag
         from a
     )
select course_id,
       course_name,
       total
from (
         select course_id,
                course_name,
                date_time,
                sum(total)
                    over (partition by course_id, course_name order by date_time asc rows between unbounded preceding and current row ) as total,
                lead(date_time, 1, '24:00:00')
                     over (partition by course_id, course_name order by date_time asc)                                                  as next_date_time
         from (
                  select course_id,
                         course_name,
                         date_time,
                         sum(flag) as total
                  from b
                  group by course_id, course_name, date_time
              ) as temp_1
     ) as temp_2
where '19:00:00' between date_time and next_date_time
order by course_id asc;

-- 26. 牛客直播各科目平均观看时长
--      观看时长 -> 离开直播间的时间与进入直播间的时间只差 -> 单位是min
--      按平均观看时长降序，结果保留两位小数

drop table if exists r5_course_tb;

drop table if exists r6_attend_tb;

create table if not exists r5_course_tb
(
    course_id       int comment '',
    course_name     string comment '',
    course_datetime string comment ''
) comment '课程表'
    row format delimited fields terminated by ','
    stored as textfile;

create table if not exists r6_attend_tb
(
    user_id      int comment '',
    course_id    int comment '',
    in_datetime  timestamp comment '',
    out_datetime timestamp comment ''
) comment '上课情况表'
    row format delimited fields terminated by ','
    stored as textfile;

INSERT INTO r5_course_tb
VALUES (1, 'Python', '2021-12-1 19:00-21:00');
INSERT INTO r5_course_tb
VALUES (2, 'SQL', '2021-12-2 19:00-21:00');
INSERT INTO r5_course_tb
VALUES (3, 'R', '2021-12-3 19:00-21:00');

INSERT INTO r6_attend_tb
VALUES (100, 1, '2021-12-1 19:00:00', '2021-12-1 19:28:00');
INSERT INTO r6_attend_tb
VALUES (100, 1, '2021-12-1 19:30:00', '2021-12-1 19:53:00');
INSERT INTO r6_attend_tb
VALUES (101, 1, '2021-12-1 19:00:00', '2021-12-1 20:55:00');
INSERT INTO r6_attend_tb
VALUES (102, 1, '2021-12-1 19:00:00', '2021-12-1 19:05:00');
INSERT INTO r6_attend_tb
VALUES (104, 1, '2021-12-1 19:00:00', '2021-12-1 20:59:00');
INSERT INTO r6_attend_tb
VALUES (101, 2, '2021-12-2 19:05:00', '2021-12-2 20:58:00');
INSERT INTO r6_attend_tb
VALUES (102, 2, '2021-12-2 18:55:00', '2021-12-2 21:00:00');
INSERT INTO r6_attend_tb
VALUES (104, 2, '2021-12-2 18:57:00', '2021-12-2 20:56:00');
INSERT INTO r6_attend_tb
VALUES (107, 2, '2021-12-2 19:10:00', '2021-12-2 19:18:00');
INSERT INTO r6_attend_tb
VALUES (100, 3, '2021-12-3 19:01:00', '2021-12-3 21:00:00');
INSERT INTO r6_attend_tb
VALUES (102, 3, '2021-12-3 18:58:00', '2021-12-3 19:05:00');
INSERT INTO r6_attend_tb
VALUES (108, 3, '2021-12-3 19:01:00', '2021-12-3 19:56:00');

-- 课程表
select *
from r5_course_tb;

-- 上课情况表
select *
from r6_attend_tb;

select r6.course_id,
       course_name,
       round(avg((unix_timestamp(out_datetime) - unix_timestamp(in_datetime)) / 60), 2) as avg_duration
from r6_attend_tb r6
         left join r5_course_tb r5
                   on r6.course_id = r5.course_id
group by r6.course_id, course_name
order by avg_duration desc;

-- 27. 牛客直播各科目出勤率
--      统计各科目出勤率 -> attend_rate(%) -> 结果保留两位小数 -> 出勤率 = 出勤人数 / 报名人数 -> 出勤人数（在线时长10min及以上）
--      按course_id升序

drop table if exists r7_course_tb;

drop table if exists r8_behavior_tb;

drop table if exists r9_attend_tb;

create table if not exists r7_course_tb
(
    course_id       int comment '',
    course_name     string comment '',
    course_datetime string comment ''
) comment '课程表'
    row format delimited fields terminated by ','
    stored as textfile;

create table if not exists r8_behavior_tb
(
    user_id   int comment '',
    if_vw     int comment '',
    if_fav    int comment '',
    if_sign   int comment '',
    course_id int comment ''
) comment '用户行为表'
    row format delimited fields terminated by ','
    stored as textfile;

create table if not exists r9_attend_tb
(
    user_id      int comment '',
    course_id    int comment '',
    in_datetime  timestamp comment '',
    out_datetime timestamp comment ''
) comment '参课表'
    row format delimited fields terminated by ','
    stored as textfile;

INSERT INTO r7_course_tb
VALUES (1, 'Python', '2021-12-1 19:00-21:00');
INSERT INTO r7_course_tb
VALUES (2, 'SQL', '2021-12-2 19:00-21:00');
INSERT INTO r7_course_tb
VALUES (3, 'R', '2021-12-3 19:00-21:00');

INSERT INTO r8_behavior_tb
VALUES (100, 1, 1, 1, 1);
INSERT INTO r8_behavior_tb
VALUES (100, 1, 1, 1, 2);
INSERT INTO r8_behavior_tb
VALUES (100, 1, 1, 1, 3);
INSERT INTO r8_behavior_tb
VALUES (101, 1, 1, 1, 1);
INSERT INTO r8_behavior_tb
VALUES (101, 1, 1, 1, 2);
INSERT INTO r8_behavior_tb
VALUES (101, 1, 0, 0, 3);
INSERT INTO r8_behavior_tb
VALUES (102, 1, 1, 1, 1);
INSERT INTO r8_behavior_tb
VALUES (102, 1, 1, 1, 2);
INSERT INTO r8_behavior_tb
VALUES (102, 1, 1, 1, 3);
INSERT INTO r8_behavior_tb
VALUES (103, 1, 1, 0, 1);
INSERT INTO r8_behavior_tb
VALUES (103, 1, 0, 0, 2);
INSERT INTO r8_behavior_tb
VALUES (103, 1, 0, 0, 3);
INSERT INTO r8_behavior_tb
VALUES (104, 1, 1, 1, 1);
INSERT INTO r8_behavior_tb
VALUES (104, 1, 1, 1, 2);
INSERT INTO r8_behavior_tb
VALUES (104, 1, 1, 0, 3);
INSERT INTO r8_behavior_tb
VALUES (105, 1, 0, 0, 1);
INSERT INTO r8_behavior_tb
VALUES (106, 1, 0, 0, 1);
INSERT INTO r8_behavior_tb
VALUES (107, 1, 0, 0, 1);
INSERT INTO r8_behavior_tb
VALUES (107, 1, 1, 1, 2);
INSERT INTO r8_behavior_tb
VALUES (108, 1, 1, 1, 3);

INSERT INTO r9_attend_tb
VALUES (100, 1, '2021-12-1 19:00:00', '2021-12-1 19:28:00')
;
INSERT INTO r9_attend_tb
VALUES (100, 1, '2021-12-1 19:30:00', '2021-12-1 19:53:00')
;
INSERT INTO r9_attend_tb
VALUES (101, 1, '2021-12-1 19:00:00', '2021-12-1 20:55:00')
;
INSERT INTO r9_attend_tb
VALUES (102, 1, '2021-12-1 19:00:00', '2021-12-1 19:05:00')
;
INSERT INTO r9_attend_tb
VALUES (104, 1, '2021-12-1 19:00:00', '2021-12-1 20:59:00')
;
INSERT INTO r9_attend_tb
VALUES (101, 2, '2021-12-2 19:05:00', '2021-12-2 20:58:00')
;
INSERT INTO r9_attend_tb
VALUES (102, 2, '2021-12-2 18:55:00', '2021-12-2 21:00:00')
;
INSERT INTO r9_attend_tb
VALUES (104, 2, '2021-12-2 18:57:00', '2021-12-2 20:56:00')
;
INSERT INTO r9_attend_tb
VALUES (107, 2, '2021-12-2 19:10:00', '2021-12-2 19:18:00')
;
INSERT INTO r9_attend_tb
VALUES (100, 3, '2021-12-3 19:01:00', '2021-12-3 21:00:00')
;
INSERT INTO r9_attend_tb
VALUES (102, 3, '2021-12-3 18:58:00', '2021-12-3 19:05:00')
;
INSERT INTO r9_attend_tb
VALUES (108, 3, '2021-12-3 19:01:00', '2021-12-3 19:56:00')
;

use firstlevel;

use secondlevel;

-- 课程表
select *
from r7_course_tb;

-- 用户行为表
select *
from r8_behavior_tb;

-- 参课表
select *
from r9_attend_tb;

with a as (
    select user_id,
           course_id,
           sum((unix_timestamp(out_datetime) - unix_timestamp(in_datetime)) / 60) as online_duration
    from r9_attend_tb
    group by user_id, course_id
),
     d as (
         select course_id, sum(if_sign) as sign_total from r8_behavior_tb group by course_id
     )
select a.course_id,
       course_name,
       round(count(`if`(online_duration >= 10, 1, null)) / (select sign_total from d where d.course_id = a.course_id) *
             100, 2) as attend_rate
from a
         left join r8_behavior_tb b
                   on a.user_id = b.user_id and a.course_id = b.course_id
         left join r7_course_tb c
                   on a.course_id = c.course_id
group by a.course_id, course_name
order by course_id asc;

-- 28. 牛客直播各科目同时在线人数
--      请你统计每个科目最大同时在线人数 -> 按course_id升序

drop table if exists r10_course_tb;

drop table if exists r11_attend_tb;

create table if not exists r10_course_tb
(
    course_id       int comment '',
    course_name     string comment '',
    course_datetime string comment ''
) comment '课程表'
    row format delimited fields terminated by ','
    stored as textfile;

create table if not exists r11_attend_tb
(
    user_id      int comment '',
    course_id    int comment '',
    in_datetime  timestamp comment '',
    out_datetime timestamp comment ''
) comment '上课情况表'
    row format delimited fields terminated by ','
    stored as textfile;

INSERT INTO r10_course_tb
VALUES (1, 'Python', '2021-12-1 19:00-21:00');
INSERT INTO r10_course_tb
VALUES (2, 'SQL', '2021-12-2 19:00-21:00');
INSERT INTO r10_course_tb
VALUES (3, 'R', '2021-12-3 19:00-21:00');

INSERT INTO r11_attend_tb
VALUES (100, 1, '2021-12-1 19:00:00', '2021-12-1 19:28:00')
;
INSERT INTO r11_attend_tb
VALUES (100, 1, '2021-12-1 19:30:00', '2021-12-1 19:53:00')
;
INSERT INTO r11_attend_tb
VALUES (101, 1, '2021-12-1 19:00:00', '2021-12-1 20:55:00')
;
INSERT INTO r11_attend_tb
VALUES (102, 1, '2021-12-1 19:00:00', '2021-12-1 19:05:00')
;
INSERT INTO r11_attend_tb
VALUES (104, 1, '2021-12-1 19:00:00', '2021-12-1 20:59:00')
;
INSERT INTO r11_attend_tb
VALUES (101, 2, '2021-12-2 19:05:00', '2021-12-2 20:58:00')
;
INSERT INTO r11_attend_tb
VALUES (102, 2, '2021-12-2 18:55:00', '2021-12-2 21:00:00')
;
INSERT INTO r11_attend_tb
VALUES (104, 2, '2021-12-2 18:57:00', '2021-12-2 20:56:00')
;
INSERT INTO r11_attend_tb
VALUES (107, 2, '2021-12-2 19:10:00', '2021-12-2 19:18:00')
;
INSERT INTO r11_attend_tb
VALUES (100, 3, '2021-12-3 19:01:00', '2021-12-3 21:00:00')
;
INSERT INTO r11_attend_tb
VALUES (102, 3, '2021-12-3 18:58:00', '2021-12-3 19:05:00')
;
INSERT INTO r11_attend_tb
VALUES (108, 3, '2021-12-3 19:01:00', '2021-12-3 19:56:00');

-- 课程表
select *
from r10_course_tb;

-- 上课情况表
select *
from r11_attend_tb;

with a as (
    select r11.course_id,
           course_name,
           in_datetime,
           out_datetime
    from r11_attend_tb r11
             left join r1_course_tb r10
                       on r11.course_id = r10.course_id
),
     b as (
         select course_id,
                course_name,
                in_datetime as date_time,
                1           as flag
         from a
         union all
         select course_id,
                course_name,
                out_datetime as date_time,
                -1           as flag
         from a
     )
select course_id,
       course_name,
       max(cnt) as max_cnt
from (
         select course_id,
                course_name,
                date_time,
                sum(flag) over (partition by course_id, course_name order by date_time asc) as cnt
         from b
     ) as table_temp
group by course_id, course_name
order by course_id asc;

-- 29. 某乎问答11月份日人均回答量
--      日人均回答量 -> 回答问题数量 / 答题人数
--      按回答日期升序，结果保留两位小数

drop table if exists s1_answer_tb;

create table if not exists s1_answer_tb
(
    answer_date date comment '',
    author_id   int comment '',
    issue_id    string comment '',
    char_len    int comment ''
) comment '问答创作者回答情况表'
    row format delimited fields terminated by ','
    stored as textfile;

INSERT INTO s1_answer_tb
VALUES ('2021-11-1', 101, 'E001', 150);
INSERT INTO s1_answer_tb
VALUES ('2021-11-1', 101, 'E002', 200);
INSERT INTO s1_answer_tb
VALUES ('2021-11-1', 102, 'C003', 50);
INSERT INTO s1_answer_tb
VALUES ('2021-11-1', 103, 'P001', 35);
INSERT INTO s1_answer_tb
VALUES ('2021-11-1', 104, 'C003', 120);
INSERT INTO s1_answer_tb
VALUES ('2021-11-1', 105, 'P001', 125);
INSERT INTO s1_answer_tb
VALUES ('2021-11-1', 102, 'P002', 105);
INSERT INTO s1_answer_tb
VALUES ('2021-11-2', 101, 'P001', 201);
INSERT INTO s1_answer_tb
VALUES ('2021-11-2', 110, 'C002', 200);
INSERT INTO s1_answer_tb
VALUES ('2021-11-2', 110, 'C001', 225);
INSERT INTO s1_answer_tb
VALUES ('2021-11-2', 110, 'C002', 220);
INSERT INTO s1_answer_tb
VALUES ('2021-11-3', 101, 'C002', 180);
INSERT INTO s1_answer_tb
VALUES ('2021-11-4', 109, 'E003', 130);
INSERT INTO s1_answer_tb
VALUES ('2021-11-4', 109, 'E001', 123);
INSERT INTO s1_answer_tb
VALUES ('2021-11-5', 108, 'C001', 160);
INSERT INTO s1_answer_tb
VALUES ('2021-11-5', 108, 'C002', 120);
INSERT INTO s1_answer_tb
VALUES ('2021-11-5', 110, 'P001', 180);
INSERT INTO s1_answer_tb
VALUES ('2021-11-5', 106, 'P002', 45);
INSERT INTO s1_answer_tb
VALUES ('2021-11-5', 107, 'E003', 56);

-- 问答创作者回答情况表
select *
from s1_answer_tb;

select answer_date,
       round(count(*) / count(distinct author_id), 2) as avg_issues
from s1_answer_tb
group by answer_date
order by answer_date asc;

-- 30. 某乎问答高质量的回答中用户属于各级别的数量
--      回答字数>=100的为高质量回答，请统计某乎高质量回答中用户属于1-2级、3-4级、5-6级的数量分别是多少
--      按数量降序

use thirdlevel;

drop table if exists s2_author_tb;

drop table if exists s3_answer_tb;

create table if not exists s2_author_tb
(
    author_id    int comment '',
    author_level int comment '',
    sex          string comment ''
) comment '问答创作者信息表'
    row format delimited fields terminated by ','
    stored as textfile;

create table if not exists s3_answer_tb
(
    answer_date date comment '',
    author_id   int comment '',
    issue_id    string comment '',
    char_len    int comment ''
) comment '问答创作者回答情况表'
    row format delimited fields terminated by ','
    stored as textfile;

INSERT INTO s2_author_tb
VALUES (101, 6, 'm');
INSERT INTO s2_author_tb
VALUES (102, 1, 'f');
INSERT INTO s2_author_tb
VALUES (103, 1, 'm');
INSERT INTO s2_author_tb
VALUES (104, 3, 'm');
INSERT INTO s2_author_tb
VALUES (105, 4, 'f');
INSERT INTO s2_author_tb
VALUES (106, 2, 'f');
INSERT INTO s2_author_tb
VALUES (107, 2, 'm');
INSERT INTO s2_author_tb
VALUES (108, 5, 'f');
INSERT INTO s2_author_tb
VALUES (109, 6, 'f');
INSERT INTO s2_author_tb
VALUES (110, 5, 'm');

INSERT INTO s3_answer_tb
VALUES ('2021-11-1', 101, 'E001', 150);
INSERT INTO s3_answer_tb
VALUES ('2021-11-1', 101, 'E002', 200);
INSERT INTO s3_answer_tb
VALUES ('2021-11-1', 102, 'C003', 50);
INSERT INTO s3_answer_tb
VALUES ('2021-11-1', 103, 'P001', 35);
INSERT INTO s3_answer_tb
VALUES ('2021-11-1', 104, 'C003', 120);
INSERT INTO s3_answer_tb
VALUES ('2021-11-1', 105, 'P001', 125);
INSERT INTO s3_answer_tb
VALUES ('2021-11-1', 102, 'P002', 105);
INSERT INTO s3_answer_tb
VALUES ('2021-11-2', 101, 'P001', 201);
INSERT INTO s3_answer_tb
VALUES ('2021-11-2', 110, 'C002', 200);
INSERT INTO s3_answer_tb
VALUES ('2021-11-2', 110, 'C001', 225);
INSERT INTO s3_answer_tb
VALUES ('2021-11-2', 110, 'C002', 220);
INSERT INTO s3_answer_tb
VALUES ('2021-11-3', 101, 'C002', 180);
INSERT INTO s3_answer_tb
VALUES ('2021-11-4', 109, 'E003', 130);
INSERT INTO s3_answer_tb
VALUES ('2021-11-4', 109, 'E001', 123);
INSERT INTO s3_answer_tb
VALUES ('2021-11-5', 108, 'C001', 160);
INSERT INTO s3_answer_tb
VALUES ('2021-11-5', 108, 'C002', 120);
INSERT INTO s3_answer_tb
VALUES ('2021-11-5', 110, 'P001', 180);
INSERT INTO s3_answer_tb
VALUES ('2021-11-5', 106, 'P002', 45);
INSERT INTO s3_answer_tb
VALUES ('2021-11-5', 107, 'E003', 56);

-- 问答创作者信息表
select *
from s2_author_tb;

-- 问答创作者回答情况表
select *
from s3_answer_tb;

select distinct case
                    when author_level between 1 and 2 then '1-2级'
                    when author_level between 3 and 4 then '3-4级'
                    else '5-6级'
                    end as user_level,
                case
                    when author_level between 1 and 2 then count(`if`(author_level between 1 and 2, 1, null)) over ()
                    when author_level between 3 and 4 then count(`if`(author_level between 3 and 4, 1, null)) over ()
                    else count(`if`(author_level between 5 and 6, 1, null)) over ()
                    end as `数量`
from s3_answer_tb s3
         left join s2_author_tb s2
                   on s3.author_id = s2.author_id
where char_len >= 100
order by `数量` desc;

-- 31. 某乎问答单日回答问题数大于等于3个的所有用户
--      统计11月份单日回答问题数大于等于3个的所有用户信息
--      用户信息 -> author_date(回答日期) -> author_id(创作者id) -> answer_cnt(回答问题个数)
--      有多条数据符合条件，按answer_date, author_id升序

drop table if exists s4_answer_tb;

create table if not exists s4_answer_tb
(
    answer_date date comment '',
    author_id   int comment '',
    issue_id    string comment '',
    char_len    int comment ''
) comment '问答创作者回答情况表'
    row format delimited fields terminated by ','
    stored as textfile;

INSERT INTO s4_answer_tb
VALUES ('2021-11-1', 101, 'E001', 150);
INSERT INTO s4_answer_tb
VALUES ('2021-11-1', 101, 'E002', 200);
INSERT INTO s4_answer_tb
VALUES ('2021-11-1', 102, 'C003', 50);
INSERT INTO s4_answer_tb
VALUES ('2021-11-1', 103, 'P001', 35);
INSERT INTO s4_answer_tb
VALUES ('2021-11-1', 104, 'C003', 120);
INSERT INTO s4_answer_tb
VALUES ('2021-11-1', 105, 'P001', 125);
INSERT INTO s4_answer_tb
VALUES ('2021-11-1', 102, 'P002', 105);
INSERT INTO s4_answer_tb
VALUES ('2021-11-2', 101, 'P001', 201);
INSERT INTO s4_answer_tb
VALUES ('2021-11-2', 110, 'C002', 200);
INSERT INTO s4_answer_tb
VALUES ('2021-11-2', 110, 'C001', 225);
INSERT INTO s4_answer_tb
VALUES ('2021-11-2', 110, 'C002', 220);
INSERT INTO s4_answer_tb
VALUES ('2021-11-3', 101, 'C002', 180);
INSERT INTO s4_answer_tb
VALUES ('2021-11-4', 109, 'E003', 130);
INSERT INTO s4_answer_tb
VALUES ('2021-11-4', 109, 'E001', 123);
INSERT INTO s4_answer_tb
VALUES ('2021-11-5', 108, 'C001', 160);
INSERT INTO s4_answer_tb
VALUES ('2021-11-5', 108, 'C002', 120);
INSERT INTO s4_answer_tb
VALUES ('2021-11-5', 110, 'P001', 180);
INSERT INTO s4_answer_tb
VALUES ('2021-11-5', 106, 'P002', 45);
INSERT INTO s4_answer_tb
VALUES ('2021-11-5', 107, 'E003', 56);

-- 问答创作者回答情况表
select *
from s4_answer_tb;

select answer_date,
       author_id,
       count(*) as answer_cnt
from s4_answer_tb
group by answer_date, author_id
having count(*) >= 3
order by answer_date asc, author_id asc;

-- 32. 某乎问答回答过教育类问题的用户里有多少用户回答过职场类问题

drop table if exists s5_issue_tb;

drop table if exists s6_answer_tb;

create table if not exists s5_issue_tb
(

    issue_id   string comment '',
    issue_type string comment ''
) comment '问答题目信息表'
    row format delimited fields terminated by ','
    stored as textfile;

create table if not exists s6_answer_tb
(
    answer_date date comment '',
    author_id   int comment '',
    issue_id    string comment '',
    char_len    int comment ''
) comment '问答创作者回答情况表'
    row format delimited fields terminated by ','
    stored as textfile;

INSERT INTO s5_issue_tb
VALUES ('E001', 'Education');
INSERT INTO s5_issue_tb
VALUES ('E002', 'Education');
INSERT INTO s5_issue_tb
VALUES ('E003', 'Education');
INSERT INTO s5_issue_tb
VALUES ('C001', 'Career');
INSERT INTO s5_issue_tb
VALUES ('C002', 'Career');
INSERT INTO s5_issue_tb
VALUES ('C003', 'Career');
INSERT INTO s5_issue_tb
VALUES ('C004', 'Career');
INSERT INTO s5_issue_tb
VALUES ('P001', 'Psychology');
INSERT INTO s5_issue_tb
VALUES ('P002', 'Psychology');

INSERT INTO s6_answer_tb
VALUES ('2021-11-1', 101, 'E001', 150);
INSERT INTO s6_answer_tb
VALUES ('2021-11-1', 101, 'E002', 200);
INSERT INTO s6_answer_tb
VALUES ('2021-11-1', 102, 'C003', 50);
INSERT INTO s6_answer_tb
VALUES ('2021-11-1', 103, 'P001', 35);
INSERT INTO s6_answer_tb
VALUES ('2021-11-1', 104, 'C003', 120);
INSERT INTO s6_answer_tb
VALUES ('2021-11-1', 105, 'P001', 125);
INSERT INTO s6_answer_tb
VALUES ('2021-11-1', 102, 'P002', 105);
INSERT INTO s6_answer_tb
VALUES ('2021-11-2', 101, 'P001', 201);
INSERT INTO s6_answer_tb
VALUES ('2021-11-2', 110, 'C002', 200);
INSERT INTO s6_answer_tb
VALUES ('2021-11-2', 110, 'C001', 225);
INSERT INTO s6_answer_tb
VALUES ('2021-11-2', 110, 'C002', 220);
INSERT INTO s6_answer_tb
VALUES ('2021-11-3', 101, 'C002', 180);
INSERT INTO s6_answer_tb
VALUES ('2021-11-4', 109, 'E003', 130);
INSERT INTO s6_answer_tb
VALUES ('2021-11-4', 109, 'E001', 123);
INSERT INTO s6_answer_tb
VALUES ('2021-11-5', 108, 'C001', 160);
INSERT INTO s6_answer_tb
VALUES ('2021-11-5', 108, 'C002', 120);
INSERT INTO s6_answer_tb
VALUES ('2021-11-5', 110, 'P001', 180);
INSERT INTO s6_answer_tb
VALUES ('2021-11-5', 106, 'P002', 45);
INSERT INTO s6_answer_tb
VALUES ('2021-11-5', 107, 'E003', 56);

-- 问答题目信息表
select *
from s5_issue_tb;

-- 问答创作者回答情况表
select *
from s6_answer_tb;

with temp_table as (
    select s5.issue_id,
           author_id,
           issue_type
    from s6_answer_tb s6
             left join s5_issue_tb s5
                       on s6.issue_id = s5.issue_id
)
select count(distinct author_id) as cnt
from temp_table
where issue_type = 'Education'
  and author_id in (
    select author_id
    from temp_table
    where issue_type = 'Career'
);

-- 33. 某乎问答最大连续回答问题天数大于等于3天的用户及其对应等级
--      有多条符合条件的数据，按author_id升序

drop table if exists s7_author_tb;

drop table if exists s8_answer_tb;

create table if not exists s7_author_tb
(
    author_id    int comment '',
    author_level int comment '',
    sex          string comment ''
) comment '问答创作者信息表'
    row format delimited fields terminated by ','
    stored as textfile;

create table if not exists s8_answer_tbs8_answer_tb
(
    answer_date date comment '',
    author_id   int comment '',
    issue_id    string comment '',
    char_len    int comment ''
) comment '问答创作者回答情况表'
    row format delimited fields terminated by ','
    stored as textfile;

INSERT INTO s7_author_tb
VALUES (101, 6, 'm');
INSERT INTO s7_author_tb
VALUES (102, 1, 'f');
INSERT INTO s7_author_tb
VALUES (103, 1, 'm');
INSERT INTO s7_author_tb
VALUES (104, 3, 'm');
INSERT INTO s7_author_tb
VALUES (105, 4, 'f');
INSERT INTO s7_author_tb
VALUES (106, 2, 'f');
INSERT INTO s7_author_tb
VALUES (107, 2, 'm');
INSERT INTO s7_author_tb
VALUES (108, 5, 'f');
INSERT INTO s7_author_tb
VALUES (109, 6, 'f');
INSERT INTO s7_author_tb
VALUES (110, 5, 'm');

INSERT INTO s8_answer_tb
VALUES ('2021-11-1', 101, 'E001', 150);
INSERT INTO s8_answer_tb
VALUES ('2021-11-1', 101, 'E002', 200);
INSERT INTO s8_answer_tb
VALUES ('2021-11-1', 102, 'C003', 50);
INSERT INTO s8_answer_tb
VALUES ('2021-11-1', 103, 'P001', 35);
INSERT INTO s8_answer_tb
VALUES ('2021-11-1', 104, 'C003', 120);
INSERT INTO s8_answer_tb
VALUES ('2021-11-1', 105, 'P001', 125);
INSERT INTO s8_answer_tb
VALUES ('2021-11-1', 102, 'P002', 105);
INSERT INTO s8_answer_tb
VALUES ('2021-11-2', 101, 'P001', 201);
INSERT INTO s8_answer_tb
VALUES ('2021-11-2', 110, 'C002', 200);
INSERT INTO s8_answer_tb
VALUES ('2021-11-2', 110, 'C001', 225);
INSERT INTO s8_answer_tb
VALUES ('2021-11-2', 110, 'C002', 220);
INSERT INTO s8_answer_tb
VALUES ('2021-11-3', 101, 'C002', 180);
INSERT INTO s8_answer_tb
VALUES ('2021-11-4', 109, 'E003', 130);
INSERT INTO s8_answer_tb
VALUES ('2021-11-4', 109, 'E001', 123);
INSERT INTO s8_answer_tb
VALUES ('2021-11-5', 108, 'C001', 160);
INSERT INTO s8_answer_tb
VALUES ('2021-11-5', 108, 'C002', 120);
INSERT INTO s8_answer_tb
VALUES ('2021-11-5', 110, 'P001', 180);
INSERT INTO s8_answer_tb
VALUES ('2021-11-5', 106, 'P002', 45);
INSERT INTO s8_answer_tb
VALUES ('2021-11-5', 107, 'E003', 56);

-- 问答创作者信息表
select *
from s7_author_tb;

-- 问答创作者回答情况表
select *
from s8_answer_tb;

select temp_2.author_id,
       author_level,
       count(date_sub(answer_date, ranking)) as cnt
from (
         select author_id,
                answer_date,
                rank() over (partition by author_id order by answer_date asc) as ranking
         from (
                  select answer_date,
                         author_id
                  from s8_answer_tb
                  group by answer_date, author_id
              ) as temp_1
     ) as temp_2
         left join s7_author_tb temp_3
                   on temp_2.author_id = temp_3.author_id
group by temp_2.author_id, author_level
having count(date_sub(answer_date, ranking)) >= 3
order by temp_2.author_id asc;

-- 34. 找出所有科目成绩都大于某一学科平均成绩的学生

drop table if exists score_info;

create table if not exists score_info
(
    uid        string comment '学生id',
    subject_id string comment '课程id',
    score      int comment '课程分数'
) comment '成绩信息表'
    row format delimited fields terminated by ','
    stored as textfile;

Insert into score_info
values ('1001', '01', 90);
Insert into score_info
values ('1001', '02', 90);
Insert into score_info
values ('1001', '03', 90);
Insert into score_info
values ('1002', '01', 85);
Insert into score_info
values ('1002', '02', 85);
Insert into score_info
values ('1002', '03', 70);
Insert into score_info
values ('1003', '01', 70);
Insert into score_info
values ('1003', '02', 70);
Insert into score_info
values ('1003', '03', 85);

-- 成绩信息表
select *
from score_info;

select uid
from (
         select uid,
                `if`(score > avg_sub, 0, 1) as flag
         from score_info a
                  left join (
             select subject_id,
                    avg(score) as avg_sub
             from score_info
             group by subject_id
         ) as b
                            on a.subject_id = b.subject_id
     ) as table_temp
group by uid
having sum(flag) = 0
order by uid asc;

-- 35. 某平台的用户访问数据
--      统计每个用户的月累计访问次数以及累计访问次数

drop table if exists action;

create table if not exists action
(
    user_id     string comment '用户id',
    visit_date  string comment '访问日期',
    visit_count int comment '访问次数'
) comment '访问信息表'
    row format delimited fields terminated by ','
    stored as textfile;

Insert into action
values ('u01', '2017/1/21', 5),
       ('u02', '2017/1/23', 6),
       ('u03', '2017/1/22', 8),
       ('u04', '2017/1/20', 3),
       ('u01', '2017/1/23', 6),
       ('u01', '2017/2/21', 8),
       ('u02', '2017/1/23', 6),
       ('u01', '2017/2/22', 4);

-- 访问信息表
select *
from action;

select user_id,
       visit_month_date,
       visit_cnt,
       sum(visit_cnt) over (partition by user_id order by visit_month_date asc) as visit_cnt_total
from (
         select user_id,
                date_format(cast(regexp_replace(visit_date, '/', '-') as timestamp), 'yyyy-MM') as visit_month_date,
                sum(visit_count)                                                                as visit_cnt
         from action
         group by user_id, date_format(cast(regexp_replace(visit_date, '/', '-') as timestamp), 'yyyy-MM')
     ) as table_temp
order by user_id asc, visit_month_date asc;

-- 36. 电商店铺数据
--      有50W个京东店铺，每个顾客访客访问任何一个店铺的任何一个商品时，都会产生一个访问日志，访问日志存储的表名。
--      求：每个店铺的UV（访客数）
--      每个店铺访问次数top3的访客信息。
--      输出店铺名称, 访客id, 访问次数

drop table if exists visit;

create table if not exists visit
(
    user_id string comment '',
    shop    string comment ''
) comment '访问信息表'
    row format delimited fields terminated by ','
    stored as textfile;

Insert into visit
values ('u1', 'a'),
       ('u2', 'b'),
       ('u1', 'b'),
       ('u1', 'a'),
       ('u3', 'c'),
       ('u4', 'b'),
       ('u1', 'a'),
       ('u2', 'c'),
       ('u5', 'b'),
       ('u4', 'b'),
       ('u6', 'c'),
       ('u2', 'c'),
       ('u1', 'b'),
       ('u2', 'a'),
       ('u2', 'a'),
       ('u3', 'a'),
       ('u5', 'a'),
       ('u5', 'a'),
       ('u5', 'a');

-- 访问信息表
select *
from visit;

select temp_1.shop,
       uv,
       user_id,
       visit_cnt
from (
         select shop,
                user_id,
                visit_cnt,
                rank() over (partition by shop order by visit_cnt desc) as ranking
         from (
                  select shop,
                         user_id,
                         count(*) as visit_cnt
                  from visit
                  group by shop, user_id
              ) as temp_table
     ) as temp_1
         left join (select shop,
                           count(distinct user_id) as uv
                    from visit
                    group by shop) as temp_2
                   on temp_1.shop = temp_2.shop
where ranking <= 3
order by temp_1.shop asc, user_id asc;
