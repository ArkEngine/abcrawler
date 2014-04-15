DROP database wenda;
CREATE database wenda;
use wenda;
CREATE TABLE `link_info` (
    `url_no`      int(10)      NOT NULL auto_increment,
    `url`         varchar(255) NOT NULL,
    `url_type`    tinyint(3)   NOT NULL,
    `host_no`     smallint(5)  NOT NULL,
    `status_code` smallint(5)  NOT NULL,
    `creat_time`  int(10)      NOT NULL,
    `check_time`  int(10)      NOT NULL,
    `refer_sign`  bigint(20)   NOT NULL,
    `url_sign`    bigint(20)   NOT NULL,
    PRIMARY KEY   (`url_no`),
    UNIQUE KEY `uniq_url_sign` (`url_sign`),
    KEY `host_no` (`host_no`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `host_info` (
    `host_no`     int(10)     NOT NULL auto_increment,
    `host`        char(64)    NOT NULL,
    PRIMARY KEY   (`host_no`),
    UNIQUE KEY `uniq_host` (`host`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
