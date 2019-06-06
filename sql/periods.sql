set search_path = public, periods;

create table vat (kind text not null, amount numeric not null, from_year integer not null, to_year integer not null);
select add_period('vat', 'validity', 'from_year', 'to_year');
select add_unique_key('vat', array['kind'], 'validity');

insert into vat values ('sales', 6, 1970, 1985);

create table tree (id integer not null, parent_id integer, other text, vstart date, vend date);
select add_period('tree', 'validity', 'vstart', 'vend');
select add_unique_key('tree', '{id,other}', 'validity');
select add_foreign_key('tree', '{parent_id,other}', 'validity', 'tree_id_other_validity');
insert into tree values (1, null, 'stinky', '2019-01-01', '2019-03-01');
insert into tree values (2, 1, 'stinky', '2019-02-01', '2019-02-04');
insert into tree values (3, 1, 'stinky', '2019-02-01', '2019-02-04');
update tree set id = 3 where id = 3;

create table dept (dno integer, dstart date, dend date, dname text);
select add_period('dept', 'dperiod', 'dstart', 'dend');
select add_unique_key('dept', '{dno}', 'dperiod');

insert into dept
values
    (3, '2009-01-01', '2011-12-31', 'Test'), 
    (4, '2011-06-01', '2011-12-31', 'QA'),
    (4, '2007-06-01', '2011-06-01', 'QA');

create table emp (eno integer, estart date, eend date, edept integer);
select add_period('emp', 'eperiod', 'estart', 'eend');
select add_unique_key('emp', '{eno}', 'eperiod');
select add_foreign_key('emp', '{edept}', 'eperiod', 'dept_dno_dperiod');

insert into emp
values
    --(22218, '1010-01-01', '1011-02-03', 3),
    (22218, '2011-02-03', '2011-11-12', 4),
    (22218, '2010-01-01', '2011-02-03', 3);

create table forsysver(id integer, val text);
select periods.add_system_time_period('forsysver');
select periods.add_system_versioning('forsysver');
insert into forsysver (id, val) values (1, 'hello'), (2, 'wyrld');
select pg_sleep_for('250ms');
update forsysver set val = 'world' where id = 2;

