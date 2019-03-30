-- create database orm_test;
use orm_test2;

DROP TABLE IF EXISTS `any_obj`;
create table if not exists any_obj(
    id int unsigned not null auto_increment,
    obj_omit text not null ,
    obj text not null ,
    primary key(id)
);

