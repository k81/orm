-- create database orm_test;
use orm_test;

DROP TABLE IF EXISTS `person`;
create table if not exists person(
    id int unsigned not null auto_increment, 
    person_id int unsigned not null default 0, 
    name varchar(255) not null default '', 
    age int not null default 0, 
    primary key(id)
);
DROP TABLE IF EXISTS `person_0`;
DROP TABLE IF EXISTS `person_1`;
DROP TABLE IF EXISTS `person_2`;
DROP TABLE IF EXISTS `person_3`;
create table if not exists person_0 like person;
create table if not exists person_1 like person;
create table if not exists person_2 like person;
create table if not exists person_3 like person;

-- grant all privileges on orm_test.* to 'orm_test'@'localhost' identified by 'orm_test';

DROP TABLE IF EXISTS `json_test`;
create table if not exists json_test(
    id int unsigned not null auto_increment,
    content VARCHAR(1024) not null default '',
    content_ptr VARCHAR(1024) not null default '',
    primary key(id)
);
DROP TABLE IF EXISTS `json_test2`;
create table if not exists json_test2 like json_test;


DROP TABLE IF EXISTS `dynamic_test`;
create table if not exists dynamic_test(
    id int unsigned not null auto_increment,
    type varchar(4) not null default '',
    content text not null ,
    primary key(id)
);

DROP TABLE IF EXISTS `any_obj`;
create table if not exists any_obj(
    id int unsigned not null auto_increment,
    obj_omit text not null ,
    obj text not null ,
    primary key(id)
);

DROP TABLE IF EXISTS `time_obj`;
create table if not exists time_obj(
    id int unsigned not null auto_increment,
    obj_time timestamp not null,
    primary key(id)
);
