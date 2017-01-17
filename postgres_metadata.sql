with param as (
select 'sbs'::text as schema
)
select s.sequence_schema as object_schema,
       s.sequence_name   as object_name,
       'sequence'        as object_type,
       query_to_xml(format('select s.* 
                      from information_schema.sequences s
                      where s.sequence_catalog = %L
                        and s.sequence_schema = %L
                        and s.sequence_name = %L'
            ,s.sequence_catalog
            ,s.sequence_schema
            ,s.sequence_name),true,true,'') as object_xml
 from information_schema.sequences s
 where s.sequence_schema = (select p.schema from param p)
union all
 select s.table_schema as object_schema, 
        s.table_name   as object_name,
        'view'        as object_type,
        query_to_xml(format('select s.* 
                      from information_schema.views s
                      where s.table_catalog= %L
                        and s.table_schema = %L
                        and s.table_name = %L'
            ,s.table_catalog
            ,s.table_schema
            ,s.table_name),false,true,'') as object_xml
     from information_schema.views s
  where s.table_schema = (select p.schema from param p)
union all
 select meta.trigger_schema as object_schema, 
        meta.trigger_name   as object_name,
        'trigger'           as object_type,
       xmlelement(name "trigger",
        xmlelement(name "header",
         query_to_xml(
          format(trg_query.query_text,
                  meta.trigger_catalog
                 ,meta.trigger_schema
                 ,meta.trigger_name)
         ,false,true,'')),
         xmlelement(name "body",
         query_to_xml(
          format('select r.* from pg_catalog.pg_trigger otr 
                    inner join pg_catalog.pg_class ot on ot.oid = otr.tgrelid 
                    inner join pg_catalog.pg_namespace otns on otns.oid = ot.relnamespace
                    inner join pg_catalog.pg_roles otown on otown.oid = otns.nspowner
                    inner join pg_catalog.pg_proc op on op.oid = otr.tgfoid
                    inner join pg_catalog.pg_namespace opns on opns.oid = op.pronamespace
                    inner join pg_catalog.pg_roles opown on opown.oid = opns.nspowner
                    inner join information_schema.routines r 
                      on r.routine_schema = opown.rolname 
                     and r.routine_name = op.proname
                   where otown.rolname = %L
                     and otr.tgname = %L'
                 ,meta.trigger_schema
                 ,meta.trigger_name)
              ,true,true,'')
          )
         )  as object_xml
from (
        select 
                 distinct 
                 t.trigger_catalog
                 ,t.trigger_schema
                 ,t.trigger_name
        from information_schema.triggers t
        where t.trigger_schema = (select p.schema from param p)
  ) as meta
  cross join (
        select concat('select '
               ,STRING_AGG(format('t.%I',t.column_name),',' order by t.ordinal_position)
               ,',STRING_AGG(t.event_manipulation,'','') as event_manipulation '
               ,' from information_schema.triggers t '
               ,' where t.trigger_catalog=%L and t.trigger_schema=%L and t.trigger_name=%L'
               ,' group by '
               ,STRING_AGG(format('t.%I',t.column_name),',' order by t.ordinal_position)
               ) as query_text
         from information_schema.columns t
         where t.table_schema = 'information_schema' 
           and t.table_name='triggers' 
           and not t.column_name = 'event_manipulation'
           ) as trg_query
union all
select t.table_schema   as object_schema,
       t.table_name     as object_name,
       'table'          as object_type,
       query_to_xml(
        format('select 
                 query_to_xml(format(%1$L,%4$L,%5$L,%6$L),false,true,'''') as table,
                 query_to_xml(format(%2$L,%4$L,%5$L,%6$L),false,true,'''') as colums,
                 query_to_xml(format(%3$L,%4$L,%5$L,%6$L),false,true,'''') as constraints
                 ',
                 /*1*/
                'select t.* from information_schema.tables t 
                  where t.table_catalog=%L
                    and t.table_schema=%L
                    and t.table_name=%L',
                /*2*/
                'select c.* from information_schema.columns c
                  where c.table_catalog=%L
                    and c.table_schema=%L
                    and c.table_name=%L',
                /*3*/
                format('select c.*, 
                           case 
                            when c.constraint_type = ''CHECK'' then 
                             query_to_xml(format(%1$L,c.constraint_catalog,c.constraint_schema,c.constraint_name),false,true,'''')
                            when c.constraint_type = ''FOREIGN KEY'' then 
                             query_to_xml(format(%2$L,c.constraint_catalog,c.constraint_schema,c.constraint_name),false,true,'''')
                           end  as constraint_extra_info,
                           case 
                            when c.constraint_type in(''PRIMARY KEY'',''FOREIGN KEY'') then 
                             query_to_xml(format(%3$L,c.constraint_catalog,c.constraint_schema,c.constraint_name),false,true,'''')
                           end as constraint_key_columns
                          from information_schema.table_constraints c %4$s',
                       /*1*/    
                       'select c.* from information_schema.check_constraints c
                         where c.constraint_catalog=%%L
                           and c.constraint_schema=%%L
                           and c.constraint_name=%%L',
                       /*2*/
                       'select c.* from information_schema.referential_constraints c
                           where c.constraint_catalog=%%L
                             and c.constraint_schema=%%L
                             and c.constraint_name=%%L',
                        /*3*/
                       'select c.* from information_schema.key_column_usage c
                           where c.constraint_catalog=%%L
                             and c.constraint_schema=%%L
                             and c.constraint_name=%%L
                        order by c.ordinal_position',
                       /*4*/
                       'where c.table_catalog=%L
                             and c.table_schema=%L
                             and c.table_name=%L'
                  ),
                 /*4,5,6*/
                 t.table_catalog,t.table_schema,t.table_name
                 ),false,true,'') as object_xml
   from information_schema.tables t where 
   t.table_schema = (select p.schema from param p)
union all
select r.routine_schema as object_schema,
       r.routine_name   as object_name,
       'function'       as object_type,
       query_to_xml(format('select s.* 
                      from information_schema.routines s
                      where s.routine_catalog = %L
                        and s.routine_schema = %L
                        and s.routine_name = %L'
            ,r.routine_catalog
            ,r.routine_schema
            ,r.routine_name),false,true,'') as object_xml
 from information_schema.routines r
 where not r.data_type = 'trigger'
   and r.routine_schema = (select p.schema from param p)
   

;  

 
---select * from information_schema.indexes