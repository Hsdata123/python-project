-- 编导表：账号名称-编导-日期 联合主键
CREATE TABLE IF NOT EXISTS `t_sucai_editor_daily_report` (
    `账号名称` VARCHAR(100) NOT NULL,
    `编导` VARCHAR(50) NOT NULL,
    `日期` VARCHAR(20) NOT NULL,
    `数据截止日期` VARCHAR(20) NOT NULL,
    `整体成交金额` DECIMAL(20,2) DEFAULT 0,
    `整体消耗` DECIMAL(20,2) DEFAULT 0,
    `整体成交订单数` INT DEFAULT 0,
    `整体展现次数` INT DEFAULT 0,
    `视频播放数` INT DEFAULT 0,
    `视频完播数` INT DEFAULT 0,
    `3秒播放次数` DECIMAL(20,2) DEFAULT 0,
    `5秒播放次数` DECIMAL(20,2) DEFAULT 0,
    `10秒播放次数` DECIMAL(20,2) DEFAULT 0,
    PRIMARY KEY (`账号名称`, `编导`, `日期`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 剪辑表：账号名称-剪辑-日期 联合主键
CREATE TABLE IF NOT EXISTS `t_sucai_clipper_daily_report` (
    `账号名称` VARCHAR(100) NOT NULL,
    `剪辑` VARCHAR(50) NOT NULL,
    `日期` VARCHAR(20) NOT NULL,
    `数据截止日期` VARCHAR(20) NOT NULL,
    `整体成交金额` DECIMAL(20,2) DEFAULT 0,
    `整体消耗` DECIMAL(20,2) DEFAULT 0,
    `整体成交订单数` INT DEFAULT 0,
    `整体展现次数` INT DEFAULT 0,
    `视频播放数` INT DEFAULT 0,
    `视频完播数` INT DEFAULT 0,
    `3秒播放次数` DECIMAL(20,2) DEFAULT 0,
    `5秒播放次数` DECIMAL(20,2) DEFAULT 0,
    `10秒播放次数` DECIMAL(20,2) DEFAULT 0,
    PRIMARY KEY (`账号名称`, `剪辑`, `日期`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;