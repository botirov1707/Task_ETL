-- Create devices table
CREATE TABLE if not exists "devices"
(
    "device_id"           BIGINT PRIMARY KEY,
    "ios_ifa"             VARCHAR(255),
    "ios_ifv"             VARCHAR(255),
    "android_id"          VARCHAR(255),
    "google_aid"          VARCHAR(255),
    "os_name"             VARCHAR(10),
    "os_version"          VARCHAR(255),
    "device_manufacturer" VARCHAR(255),
    "device_model"        VARCHAR(255),
    "device_type"         VARCHAR(10) NOT NULL,
    "is_bot"              BOOLEAN NOT NULL,
    "country_iso_code"    VARCHAR(2) NOT NULL,
    "city"                VARCHAR(60)
);

CREATE TABLE if not exists "publishers"
(
    "publisher_id"   BIGINT PRIMARY KEY,
    "publisher_name" VARCHAR(255) NOT NULL
);

CREATE TABLE if not exists "trackers"
(
    "tracking_id"  BIGINT PRIMARY KEY,
    "tracker_name" TEXT NOT NULL
);

CREATE TABLE if not exists "clicks"
(
    "click_id"             BIGINT ,
    "click_ipv6"           VARCHAR(39),
    "click_timestamp"      BIGINT NOT NULL,
    "click_url_parameters" TEXT,
    "click_user_agent"     TEXT,
    "device_id"            BIGINT,
    "publisher_id"         BIGINT,
    "tracking_id"          BIGINT
);
